package main

import (
	"github.com/gin-gonic/gin"
)

import (
	_ "github.com/transaction-wg/seata-golang/pkg/base/registry/file"
	_ "github.com/transaction-wg/seata-golang/pkg/base/registry/nacos"

	"github.com/transaction-wg/seata-golang/pkg/client"
	"github.com/transaction-wg/seata-golang/pkg/client/config"
	"github.com/transaction-wg/seata-golang/pkg/client/tcc"
	"github.com/transaction-wg/seata-golang/pkg/client/tm"
	"github.com/transaction-wg/seata-golang/samples/tcc/service"
)

/**
1. 先启动tc，再启动rm
http://localhost:8080/commit
http://localhost:8080/rollback
 */
func main() {
	r := gin.Default()
//	config.InitConf()
	config.InitConfWithDefault("testService")
	client.NewRpcClient()
	// todo 使用tcc模式，收到branchCommit/branchRollback时，分别调用confirm/cancel方法，所以这里不需要在resourceManager设置undo_log
	tcc.InitTCCResourceManager()

	// todo tm代理，走tm的begin/commit/rollback，如果报错，会rollback
	tm.Implement(service.ProxySvc)
	// todo try方法代理，单独注册分支事务
	tcc.ImplementTCC(service.TccProxyServiceA)
	tcc.ImplementTCC(service.TccProxyServiceB)
	tcc.ImplementTCC(service.TccProxyServiceC)

	r.GET("/commit", func(c *gin.Context) {
		// todo tm代理，测试正常提交
		service.ProxySvc.TCCCommitted(c)
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	// todo tm代理，测试抛出异常回滚
	r.GET("/rollback", func(c *gin.Context) {
		service.ProxySvc.TCCCanceled(c)
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.Run()
}
