package main

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/transaction-wg/seata-golang/pkg/util/log"
)

import (
	"github.com/gin-gonic/gin"
)

import (
	_ "github.com/transaction-wg/seata-golang/pkg/base/config_center/nacos"
	_ "github.com/transaction-wg/seata-golang/pkg/base/registry/file"
	_ "github.com/transaction-wg/seata-golang/pkg/base/registry/nacos"
	"github.com/transaction-wg/seata-golang/pkg/client"
	"github.com/transaction-wg/seata-golang/pkg/client/at/exec"
	"github.com/transaction-wg/seata-golang/pkg/client/config"
	"github.com/transaction-wg/seata-golang/pkg/client/context"
	"github.com/transaction-wg/seata-golang/samples/at/order_svc/dao"
)

// go run main.go  -conConf conf/client.yml

func main() {
	r := gin.Default()
	if err := config.InitConf(); err != nil {
		panic(err)
	}
	client.NewRpcClient()
	exec.InitDataResourceManager()

	sqlDB, err := sql.Open("mysql", config.GetATConfig().DSN)
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(4 * time.Hour)

	db, err := exec.NewDB(config.GetATConfig(), sqlDB)
	if err != nil {
		panic(err)
	}
	d := &dao.Dao{
		DB: db,
	}

	r.POST("/createSo", func(c *gin.Context) {
		type req struct {
			Req []*dao.SoMaster
		}
		var q req
		if err := c.ShouldBindJSON(&q); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		rootContext := &context.RootContext{Context: c}
		rootContext.Bind(c.Request.Header.Get("XID"))

		log.Info("receive request ")
		_, err := d.CreateSO(rootContext, q.Req)

		if err != nil {
			c.JSON(400, gin.H{
				"success": false,
				"message": err.Error(),
			})
		} else {
			c.JSON(200, gin.H{
				"success": true,
				"message": "success",
			})
		}
	})
	r.Run(":8002")
}
