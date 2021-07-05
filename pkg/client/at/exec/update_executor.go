package exec

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/transaction-wg/seata-golang/pkg/util/log"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/transaction-wg/seata-golang/pkg/client/at/proxy_tx"
	"github.com/transaction-wg/seata-golang/pkg/client/at/sql/schema"
	"github.com/transaction-wg/seata-golang/pkg/client/at/sql/schema/cache"
	"github.com/transaction-wg/seata-golang/pkg/client/at/sqlparser"
	"github.com/transaction-wg/seata-golang/pkg/util/mysql"
	sql2 "github.com/transaction-wg/seata-golang/pkg/util/sql"
)

type UpdateExecutor struct {
	proxyTx       *proxy_tx.ProxyTx
	sqlRecognizer sqlparser.ISQLUpdateRecognizer
	values        []interface{}
}

func (executor *UpdateExecutor) Execute() (sql.Result, error) {
	// todo 执行前结果，加了for update
	beforeImage, err := executor.BeforeImage()
	if err != nil {
		return nil, err
	}
	// todo 执行真正的sql
	result, err := executor.proxyTx.Exec(executor.sqlRecognizer.GetOriginalSQL(), executor.values...)
	if err != nil {
		return result, err
	}
	// todo 执行后的结果
	afterImage, err := executor.AfterImage(beforeImage)
	if err != nil {
		return nil, err
	}
	executor.PrepareUndoLog(beforeImage, afterImage)
	return result, err
}

func (executor *UpdateExecutor) PrepareUndoLog(beforeImage, afterImage *schema.TableRecords) {
	if len(beforeImage.Rows) == 0 &&
		(afterImage == nil || len(afterImage.Rows) == 0) {
		return
	}

	var lockKeyRecords = afterImage

	lockKeys := buildLockKey(lockKeyRecords)
	executor.proxyTx.AppendLockKey(lockKeys)

	sqlUndoLog := buildUndoItem(executor.sqlRecognizer, beforeImage, afterImage)
	executor.proxyTx.AppendUndoLog(sqlUndoLog)
}

func (executor *UpdateExecutor) BeforeImage() (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	return executor.buildTableRecords(tableMeta)
}

func (executor *UpdateExecutor) AfterImage(beforeImage *schema.TableRecords) (*schema.TableRecords, error) {
	if beforeImage.Rows == nil || len(beforeImage.Rows) == 0 {
		return nil, nil
	}

	tableMeta, err := executor.getTableMeta()
	if err != nil {
		return nil, err
	}
	afterImageSql := executor.buildAfterImageSql(tableMeta, beforeImage)
	log.Info("更新后数据：%s", afterImageSql)
	var args = make([]interface{}, 0)
	for _, field := range beforeImage.PkFields() {
		args = append(args, field.Value)
	}
	rows, err := executor.proxyTx.Query(afterImageSql, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return schema.BuildRecords(tableMeta, rows), nil
}

func (executor *UpdateExecutor) getTableMeta() (schema.TableMeta, error) {
	tableMetaCache := cache.GetTableMetaCache()
	return tableMetaCache.GetTableMeta(executor.proxyTx.Tx, executor.sqlRecognizer.GetTableName(), executor.proxyTx.ResourceID)
}

func (executor *UpdateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(column))
		i = i + 1
		if i != columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", executor.sqlRecognizer.GetTableName())
	fmt.Fprint(&b, executor.sqlRecognizer.GetWhereCondition())
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *UpdateExecutor) buildAfterImageSql(tableMeta schema.TableMeta, beforeImage *schema.TableRecords) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		fmt.Fprint(&b, mysql.CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s ", executor.sqlRecognizer.GetTableName())
	fmt.Fprintf(&b, "WHERE `%s` IN", tableMeta.GetPkName())
	// todo 根据老数据的主键查询新结果
	fmt.Fprint(&b, sql2.AppendInParam(len(beforeImage.PkFields())))
	return b.String()
}

func (executor *UpdateExecutor) buildTableRecords(tableMeta schema.TableMeta) (*schema.TableRecords, error) {
	sql := executor.buildBeforeImageSql(tableMeta)
	argsCount := strings.Count(sql, "?")
	log.Info("query before image:%#v", sql)
	rows, err := executor.proxyTx.Query(sql, executor.values[len(executor.values)-argsCount:]...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return schema.BuildRecords(tableMeta, rows), nil
}
