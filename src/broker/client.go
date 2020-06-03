package broker

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"awesomeProject/beacon/mqtt-broker-sn/broker/lib/sessions"
	"awesomeProject/beacon/mqtt-broker-sn/broker/lib/topics"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
)

const (
	_GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`
)

const (
	Connected    = 1
	Disconnected = 2
)

//Same as three type of the fixed work pools : Regular/Special/Supreme
const (
	RegularLevel int8 = 1 << iota
	SpecialLevel
	SupremeLevel
)

var (
	groupCompile = regexp.MustCompile(_GroupTopicRegexp)
)

type client struct {
	mu         sync.Mutex
	broker     *Broker
	conn       net.Conn
	info       info
	status     int
	ctx        context.Context
	cancelFunc context.CancelFunc
	session    *sessions.Session
	subMap     map[string]*subscription
	topicsMgr  *topics.Manager
	subs       []interface{}
	qoss       []byte
	rmsgs      []*packets.PublishPacket
	//routeSubMap map[string]uint64
}

type subscription struct {
	client    *client
	topic     string
	qos       byte
	share     bool
	groupName string
}

type info struct {
	clientID  string
	username  string
	password  []byte
	keepalive uint16
	willMsg   *packets.PublishPacket
	localIP   string
	remoteIP  string
	msgLevel  int8
}

var (
	DisconnectdPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
	r                 = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func (c *client) init() {
	c.status = Connected

	c.info.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.info.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
	c.info.msgLevel = RegularLevel

	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	c.subMap = make(map[string]*subscription)
	c.topicsMgr = c.broker.topicsMgr
}

func (c *client) readLoop() {
	nc := c.conn
	b := c.broker
	if nc == nil || b == nil {
		return
	}

	keepAlive := time.Second * time.Duration(c.info.keepalive)
	timeOut := keepAlive + (keepAlive / 2)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			//add read timeout
			if err := nc.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
				log.Error("client/readLoop: set read timeout error => ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				msg := &Message{
					client: c,
					packet: DisconnectdPacket,
				}
				b.SubmitWork(c.info.clientID, msg)
				return
			}

			packet, err := packets.ReadPacket(nc)
			if err != nil {
				log.Error("client/readLoop: read packet error => ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				msg := &Message{
					client: c,
					packet: DisconnectdPacket,
				}
				b.SubmitWork(c.info.clientID, msg)
				return
			}

			msg := &Message{
				client: c,
				packet: packet,
			}
			b.SubmitWork(c.info.clientID, msg)
		}
	}

}

func ProcessMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if ca == nil {
		return
	}

	log.Debug("client/ProcessMessage: Recv message => ", zap.String("message type", reflect.TypeOf(msg.packet).String()[9:]), zap.Int8("message level", c.info.msgLevel), zap.String("ClientID", c.info.clientID))

	switch ca.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := ca.(*packets.PublishPacket)
		c.ProcessPublish(packet)
	case *packets.PubackPacket:
	case *packets.PubrecPacket:
	case *packets.PubrelPacket:
	case *packets.PubcompPacket:
	case *packets.SubscribePacket:
		packet := ca.(*packets.SubscribePacket)
		c.ProcessSubscribe(packet)
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := ca.(*packets.UnsubscribePacket)
		c.ProcessUnSubscribe(packet)
	case *packets.UnsubackPacket:
	case *packets.PingreqPacket:
		c.ProcessPing()
	case *packets.PingrespPacket:
	case *packets.DisconnectPacket:
		c.Close()
	default:
		log.Info("client/ProcessMessage: Recv unknown message .......", zap.Int8("message level", c.info.msgLevel), zap.String("ClientID", c.info.clientID))
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	c.processClientPublish(packet)
}

func (c *client) processClientPublish(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	/*
		topic := packet.TopicName

		if !c.broker.CheckTopicAuth(PUB, c.info.username, topic) {
			log.Error("Pub Topics Auth failed, ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
			return
		}

		//publish kafka
		c.broker.Publish(&plugins.Elements{
			ClientID:  c.info.clientID,
			Username:  c.info.username,
			Action:    plugins.Publish,
			Timestamp: time.Now().Unix(),
			Payload:   string(packet.Payload),
			Topic:     topic,
		})*/

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("client/processClientPublish: send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		return
	default:
		log.Error("client/processClientPublish: publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}

}

func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) {
	b := c.broker
	if b == nil {
		return
	}

	if packet.Retain {
		if err := c.topicsMgr.Retain(packet); err != nil {
			log.Error("client/ProcessPublishMessage: Error retaining message => ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}

	err := c.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &c.subs, &c.qoss)
	if err != nil {
		log.Error("client/ProcessPublishMessage: Error retrieving subscribers list => ", zap.String("ClientID", c.info.clientID))
		return
	}

	// fmt.Println("psubs num: ", len(c.subs))
	if len(c.subs) == 0 {
		return
	}

	var qsub []int
	for i, sub := range c.subs {
		s, ok := sub.(*subscription)
		if ok {
			if s.share {
				qsub = append(qsub, i)
			} else {
				publish(s, packet)
			}
		}

	}

	if len(qsub) > 0 {
		idx := r.Intn(len(qsub))
		sub := c.subs[qsub[idx]].(*subscription)
		publish(sub, packet)
	}

}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	c.processClientSubscribe(packet)
}

func (c *client) processClientSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics
	qoss := packet.Qoss

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for i, topic := range topics {
		t := topic
		/*
			//check topic auth for client
			if !b.CheckTopicAuth(SUB, c.info.username, topic) {
				log.Error("Sub topic Auth failed: ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
				retcodes = append(retcodes, QosFailure)
				continue
			}

			b.Publish(&plugins.Elements{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Action:    plugins.Subscribe,
				Timestamp: time.Now().Unix(),
				Topic:     topic,
			})*/

		groupName := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				retcodes = append(retcodes, QosFailure)
				continue
			}
			share = true
			groupName = substr[1]
			topic = substr[2]
		}

		sub := &subscription{
			topic:     topic,
			qos:       qoss[i],
			client:    c,
			share:     share,
			groupName: groupName,
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			log.Error("subscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		c.subMap[t] = sub

		_ = c.session.AddTopic(t, qoss[i])
		retcodes = append(retcodes, rqos)
		_ = c.topicsMgr.Retained([]byte(topic), &c.rmsgs)

	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}

	//process retain message
	for _, rm := range c.rmsgs {
		if err := c.WriterPacket(rm); err != nil {
			log.Error("Error publishing retained message:", zap.Any("err", err), zap.String("ClientID", c.info.clientID))
		} else {
			log.Info("process retain  message: ", zap.Any("packet", packet), zap.String("ClientID", c.info.clientID))
		}
	}
}

func (c *client) ProcessUnSubscribe(packet *packets.UnsubscribePacket) {
	c.processClientUnSubscribe(packet)
}

func (c *client) processClientUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}
	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics

	for _, topic := range topics {
		/*{
			//publish kafka

			b.Publish(&plugins.Elements{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Action:    plugins.Unsubscribe,
				Timestamp: time.Now().Unix(),
				Topic:     topic,
			})

		}*/

		sub, exist := c.subMap[topic]
		if exist {
			_ = c.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			_ = c.session.RemoveTopic(topic)
			delete(c.subMap, topic)
		}

	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) ProcessPing() {
	if c.status == Disconnected {
		return
	}
	resp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := c.WriterPacket(resp)
	if err != nil {
		log.Error("send PingResponse error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) Close() {
	if c.status == Disconnected {
		return
	}

	c.cancelFunc()

	c.status = Disconnected
	//wait for message complete
	// time.Sleep(1 * time.Second)
	// c.status = Disconnected

	b := c.broker
	/*
		b.Publish(&plugins.Elements{
			ClientID:  c.info.clientID,
			Username:  c.info.username,
			Action:    plugins.Disconnect,
			Timestamp: time.Now().Unix(),
		})*/

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	subs := c.subMap

	if b != nil {
		b.removeClient(c)
		for _, sub := range subs {
			err := b.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			if err != nil {
				log.Error("unsubscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			}
		}

		//offline notification
		b.OnlineOfflineNotification(c.info.clientID, false)

		if c.info.willMsg != nil {
			b.PublishMessage(c.info.willMsg)
		}
	}
}

func (c *client) WriterPacket(packet packets.ControlPacket) error {
	if c.status == Disconnected {
		return nil
	}

	if packet == nil {
		return nil
	}
	if c.conn == nil {
		c.Close()
		return errors.New("client/WriterPacket: connect lost")
	}

	c.mu.Lock()
	err := packet.Write(c.conn)
	c.mu.Unlock()
	return err
}
