package server

import (
	getty "github.com/apache/dubbo-getty"
	"time"
)

import (
	getty2 "github.com/transaction-wg/seata-golang/pkg/base/getty"
	"github.com/transaction-wg/seata-golang/pkg/base/protocal"
	"github.com/transaction-wg/seata-golang/pkg/util/log"
)

const (
	CronPeriod = 20e9
)
// todo 核心监听器
func (coordinator *DefaultCoordinator) OnOpen(session getty.Session) error {
	log.Infof("got getty_session:%s", session.Stat())
	return nil
}

func (coordinator *DefaultCoordinator) OnError(session getty.Session, err error) {
	SessionManager.ReleaseGettySession(session)
	session.Close()
	log.Errorf("getty_session{%s} got error{%v}, will be closed.", session.Stat(), err)
}

func (coordinator *DefaultCoordinator) OnClose(session getty.Session) {
	log.Info("getty_session{%s} is closing......", session.Stat())
}

func (coordinator *DefaultCoordinator) OnMessage(session getty.Session, pkg interface{}) {
	log.Debugf("received message:{%#v}", pkg)
	rpcMessage, ok := pkg.(protocal.RpcMessage)
	if ok {
		_, isRegTM := rpcMessage.Body.(protocal.RegisterTMRequest)
		if isRegTM {
			// todo 收到tm注册消息
			coordinator.OnRegTmMessage(rpcMessage, session)
			return
		}

		heartBeat, isHeartBeat := rpcMessage.Body.(protocal.HeartBeatMessage)
		if isHeartBeat && heartBeat == protocal.HeartBeatMessagePing {
			// todo 收到ping请求
			coordinator.OnCheckMessage(rpcMessage, session)
			return
		}

		// todo 收到请求
		if rpcMessage.MessageType == protocal.MSGTYPE_RESQUEST ||
			rpcMessage.MessageType == protocal.MSGTYPE_RESQUEST_ONEWAY {
			log.Debugf("msgID:%s, body:%v", rpcMessage.ID, rpcMessage.Body)
			_, isRegRM := rpcMessage.Body.(protocal.RegisterRMRequest)
			if isRegRM {
				// todo 收到rm注册消息
				coordinator.OnRegRmMessage(rpcMessage, session)
			} else {
				if SessionManager.IsRegistered(session) {
					defer func() {
						if err := recover(); err != nil {
							log.Errorf("Catch Exception while do RPC, request: %v,err: %w", rpcMessage, err)
						}
					}()
					coordinator.OnTrxMessage(rpcMessage, session)
				} else {
					session.Close()
					log.Infof("close a unhandled connection! [%v]", session)
				}
			}
		} else {
			// todo 收到响应，唤醒阻塞的goroutine
			resp, loaded := coordinator.futures.Load(rpcMessage.ID)
			if loaded {
				response := resp.(*getty2.MessageFuture)
				response.Response = rpcMessage.Body
				response.Done <- true
				coordinator.futures.Delete(rpcMessage.ID)
			}
		}
	}
}

func (coordinator *DefaultCoordinator) OnCron(session getty.Session) {
	active := session.GetActive()
	if CronPeriod < time.Since(active).Nanoseconds() {
		log.Infof("OnCorn session{%s} timeout{%s}", session.Stat(), time.Since(active).String())
		session.Close()
	}
}
