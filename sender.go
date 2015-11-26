package mns

import (
	"bytes"
	"github.com/gogap/ali_mns"
	"github.com/rs/xid"
	"strings"
	"sync"
	"time"

	"github.com/gogap/spirit"
)

const (
	mnsSenderURN = "urn:spirit-contrib:sender:mns"

	mnsSenderFlowMetadata           = "urn:spirit-contrib:sender:mns#flow"
	mnsSenderQueueURNMetadata       = "urn:spirit-contrib:sender:mns#queue_urn"
	mnsSenderQueueParallelIdContext = "urn:spirit-contrib:sender:mns#parallel"
)

var _ spirit.TranslatorSender = new(MNSSender)

type _Deliveries struct {
	Deliveries []spirit.Delivery
	Error      error
}

type MNSSenderConfig struct {
	URL             string `json:"url"`
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	DelaySeconds    int    `json:"delay_seconds"`
	Priority        int    `json:"priority"`
	ProxyAddress    string `json:"proxy_address"`
}

type MNSSender struct {
	statusLocker sync.Mutex
	terminaled   chan bool
	conf         MNSSenderConfig

	status spirit.Status

	mnsClient    map[string]ali_mns.AliMNSQueue
	clientLocker sync.Mutex

	translator spirit.OutputTranslator

	getter spirit.DeliveryGetter
}

func init() {
	spirit.RegisterSender(mnsSenderURN, NewMNSSender)
}

func NewMNSSender(config spirit.Map) (sender spirit.Sender, err error) {
	conf := MNSSenderConfig{}
	if err = config.ToObject(&conf); err != nil {
		return
	}

	if conf.URL == "" {
		err = ErrAliURLIsEmpty
		return
	}

	if conf.AccessKeyId == "" {
		err = ErrAliAccessKeyIdIsEmpty
		return
	}

	if conf.AccessKeySecret == "" {
		err = ErrAliAccessKeySecretIsEmpty
		return
	}

	if conf.Priority == 0 {
		conf.Priority = 8
	}

	if conf.DelaySeconds > 604800 {
		conf.DelaySeconds = 604800
	}

	if conf.DelaySeconds < 0 {
		conf.DelaySeconds = 0
	}

	sender = &MNSSender{
		conf:       conf,
		mnsClient:  make(map[string]ali_mns.AliMNSQueue),
		terminaled: make(chan bool),
	}

	return
}

func (p *MNSSender) Start() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	if p.status == spirit.StatusRunning {
		err = spirit.ErrSenderAlreadyRunning
		return
	}

	spirit.Logger().WithField("actor", spirit.ActorSender).
		WithField("urn", mnsSenderURN).
		WithField("event", "start").
		Infoln("enter start")

	if p.getter == nil {
		err = spirit.ErrSenderDeliveryGetterIsNil
		return
	}

	p.terminaled = make(chan bool)

	p.status = spirit.StatusRunning

	go func() {
		for {
			if deliveries, err := p.getter.Get(); err != nil {
				spirit.Logger().
					WithField("actor", spirit.ActorSender).
					WithField("urn", mnsSenderURN).
					WithField("event", "get delivery from getter").
					Errorln(err)
			} else {
				for _, delivery := range deliveries {
					if metadata := delivery.Metadata(); metadata != nil {
						flow := MNSFlowMetadata{}
						if err := metadata.Object(mnsSenderFlowMetadata, &flow); err != nil {
							spirit.Logger().
								WithField("actor", spirit.ActorSender).
								WithField("urn", mnsSenderURN).
								WithField("event", "convert metadata to mns_flow failed").
								Errorln(err)

							continue
						}
						p.callFlow(delivery, &flow)
					} else {
						spirit.Logger().
							WithField("actor", spirit.ActorSender).
							WithField("urn", mnsSenderURN).
							WithField("event", "get metadata").
							Warnln("metadata not exist")

						continue
					}
				}
			}

			select {
			case signal := <-p.terminaled:
				{
					if signal == true {
						spirit.Logger().WithField("actor", spirit.ActorSender).
							WithField("urn", mnsSenderURN).
							WithField("event", "terminal").
							Debugln("terminal singal received")
						return
					}
				}
			case <-time.After(time.Microsecond * 10):
				{
					continue
				}
			}
		}
	}()

	return
}

func (p *MNSSender) Stop() (err error) {
	p.statusLocker.Lock()
	defer p.statusLocker.Unlock()

	if p.status == spirit.StatusStopped {
		err = spirit.ErrSenderDidNotRunning
		return
	}

	spirit.Logger().WithField("actor", spirit.ActorSender).
		WithField("urn", mnsSenderURN).
		WithField("event", "stop").
		Infoln("enter stop")

	p.terminaled <- true

	close(p.terminaled)

	spirit.Logger().WithField("actor", spirit.ActorSender).
		WithField("urn", mnsSenderURN).
		WithField("event", "stop").
		Infoln("stopped")

	return
}

func (p *MNSSender) Status() spirit.Status {
	return p.status
}

func (p *MNSSender) SetDeliveryGetter(getter spirit.DeliveryGetter) (err error) {
	p.getter = getter
	return
}

func (p *MNSSender) getMNSClient(queue string) ali_mns.AliMNSQueue {
	p.clientLocker.Lock()
	defer p.clientLocker.Unlock()

	if q, exist := p.mnsClient[queue]; exist {
		return q
	}

	cli := ali_mns.NewAliMNSClient(p.conf.URL, p.conf.AccessKeyId, p.conf.AccessKeySecret)
	if p.conf.ProxyAddress != "" {
		cli.SetProxy(p.conf.ProxyAddress)
	}

	q := ali_mns.NewMNSQueue(queue, cli)

	p.mnsClient[queue] = q

	return q
}

func (p *MNSSender) callFlow(delivery spirit.Delivery, flowMetadata *MNSFlowMetadata) {
	if flowMetadata == nil {
		return
	}

	sendDeliveryFunc := func(ignoreSendError bool, delivery spirit.Delivery, queueURN map[string]string, queues ...string) (err error) {
		if queues == nil || len(queues) == 0 {
			return
		}

		backupURN := delivery.URN()
		backupParallel := new(MNSParallelFlowMetadata)
		delivery.Payload().Context().Object(mnsSenderQueueParallelIdContext, backupParallel)

		// recover
		defer func() {
			delivery.SetURN(backupURN)
			delivery.Payload().SetContext(mnsSenderQueueParallelIdContext, backupParallel)
		}()

		for _, q := range queues {

			if q == "" {
				continue
			}

			parallelQueues := strings.Split(q, "||")
			parallelCount := len(parallelQueues)
			parallelQueuePid := ""

			if parallelCount > 0 {
				parallelQueuePid = xid.New().String()
			}

			for index, queue := range parallelQueues {

				// recover
				delivery.SetURN(backupURN)
				delivery.Payload().SetContext(mnsSenderQueueParallelIdContext, backupParallel)

				urn := ""
				if queueURN != nil {
					urn, _ = queueURN[queue]
				}

				buf := bytes.Buffer{}
				delivery.SetURN(urn)

				if parallelCount > 0 {
					delivery.Payload().SetContext(mnsSenderQueueParallelIdContext, MNSParallelFlowMetadata{Id: parallelQueuePid, Index: index, Count: parallelCount})
				}

				if err = p.translator.Out(&buf, delivery); err != nil {
					spirit.Logger().
						WithField("actor", spirit.ActorSender).
						WithField("urn", mnsSenderURN).
						WithField("event", "translate delivery").
						Errorln(err)
					return
				} else {
					reqData := ali_mns.MessageSendRequest{
						MessageBody:  ali_mns.Base64Bytes(buf.Bytes()),
						DelaySeconds: int64(p.conf.DelaySeconds),
						Priority:     int64(p.conf.Priority),
					}

					client := p.getMNSClient(queue)
					if _, err = client.SendMessage(reqData); err != nil {

						spirit.Logger().
							WithField("actor", spirit.ActorSender).
							WithField("urn", mnsSenderURN).
							WithField("event", "send mns message").
							Errorln(err)

						if ignoreSendError {
							err = nil
							continue
						}
						return
					}
				}
			}

		}
		return
	}

	currentQueueURN := map[string]string{}
	if err := delivery.Metadata().Object(mnsSenderQueueURNMetadata, &currentQueueURN); err != nil {
		spirit.Logger().
			WithField("actor", spirit.ActorSender).
			WithField("urn", mnsSenderURN).
			WithField("event", "get queue urn").
			Errorln(err)
	}

	if delivery.Payload().LastError() != nil {
		// run error flow

		if flowMetadata.Error != nil && len(flowMetadata.Error) > 0 {

			queueURN := map[string]string{}

			for _, queue := range flowMetadata.Error {
				if urn, exist := currentQueueURN[queue]; exist {
					queueURN[queue] = urn
				}
			}

			delivery.SetMetadata(mnsSenderFlowMetadata, nil)
			sendDeliveryFunc(true, delivery, queueURN, flowMetadata.Error...)
		}

	} else {
		// run normal flow
		if flowMetadata.Normal != nil && len(flowMetadata.Normal) > 0 {
			nextQueue := flowMetadata.Normal[flowMetadata.CurrentFlowId]

			queueURN := map[string]string{}

			if len(flowMetadata.Normal) > flowMetadata.CurrentFlowId {
				tmpRemainQueues := flowMetadata.Normal[flowMetadata.CurrentFlowId:]

				remainQueues := []string{}
				for _, queue := range tmpRemainQueues {
					queues := strings.Split(queue, "||")
					remainQueues = append(remainQueues, queues...)
				}

				for _, queue := range remainQueues {
					if urn, exist := currentQueueURN[queue]; exist {
						queueURN[queue] = urn
					}
				}
			}

			flowMetadata.CurrentFlowId += 1
			defer func() { flowMetadata.CurrentFlowId -= 1 }()

			delivery.SetMetadata(mnsSenderFlowMetadata, flowMetadata)
			sendDeliveryFunc(false, delivery, queueURN, nextQueue)
		}
	}
}

func (p *MNSSender) SetTranslator(translator spirit.OutputTranslator) (err error) {
	p.translator = translator
	return
}
