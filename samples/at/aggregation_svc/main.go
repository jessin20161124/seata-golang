package main

import (
	"github.com/gin-gonic/gin"
)

import (
	_ "github.com/transaction-wg/seata-golang/pkg/base/config_center/nacos"
	_ "github.com/transaction-wg/seata-golang/pkg/base/registry/file"
	_ "github.com/transaction-wg/seata-golang/pkg/base/registry/nacos"
	"github.com/transaction-wg/seata-golang/pkg/client"
	"github.com/transaction-wg/seata-golang/pkg/client/config"
	"github.com/transaction-wg/seata-golang/pkg/client/tm"
	"github.com/transaction-wg/seata-golang/samples/at/aggregation_svc/svc"
)

// http://localhost:8003/createSoCommit
// http://localhost:8003/createSoRollback

func main() {
	r := gin.Default()
	config.InitConf()
	client.NewRpcClient()
	tm.Implement(svc.ProxySvc)

	r.GET("/createSoCommit", func(c *gin.Context) {

		err := svc.ProxySvc.CreateSo(c, false)

		if err != nil {
			c.JSON(500, gin.H{
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

	r.GET("/createSoRollback", func(c *gin.Context) {

		err := svc.ProxySvc.CreateSo(c, true)

		if err != nil {
			c.JSON(500, gin.H{
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

	r.Run(":8003")
}
