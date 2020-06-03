package broker

import (
	"crypto/tls"
	"fmt"
	"github.com/shirou/gopsutil/mem"
	"golang.org/x/net/websocket"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"awesomeProject/beacon/mqtt-broker-sn/broker/lib/acl"
	"awesomeProject/beacon/mqtt-broker-sn/broker/lib/sessions"
	"awesomeProject/beacon/mqtt-broker-sn/broker/lib/topics"
	"awesomeProject/beacon/mqtt-broker-sn/pool"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
)

type Message struct {
	client *client
	packet packets.ControlPacket
}

type Broker struct {
	id         string
	mu         sync.Mutex
	config     *Config
	tlsConfig  *tls.Config
	AclConfig  *acl.ACLConfig
	mfwPools   *pool.MultiFixedWorkPools
	clients    sync.Map
	topicsMgr  *topics.Manager
	sessionMgr *sessions.Manager
}

func ValidPoolLevel(level int8) bool {
	return level == RegularLevel || level == SpecialLevel || level == SupremeLevel
}
func CheckTopicForPoolLevel(topic string) int8 {
	level := int8(0)

	if len(topic) <= 10 {
		if strings.HasPrefix(topic, "$SYS") {
			return SpecialLevel // $SYS use the Special Level
		} else {
			return level
		}
	}
	switch topic[:10] {
	case "*Regular*/":
		level = RegularLevel
	case "*Special*/":
		level = SpecialLevel
	case "*Supreme*/":
		level = SupremeLevel
	default:
		level = 0
	}
	return level
}

func CheckTopicsForPoolLevel(topics []string) int8 {
	level := int8(0)
	for _, topic := range topics {
		lv := CheckTopicForPoolLevel(topic)
		if lv > level {
			level = lv
		}
		if level == SupremeLevel {
			return level
		}
	}
	return level
}
func (b *Broker) GetTaskPoolLevel(msg *Message) int8 {
	level := int8(0)
	ca := msg.packet
	switch ca.(type) {
	case *packets.PublishPacket:
		level = CheckTopicForPoolLevel(ca.(*packets.PublishPacket).TopicName)
	case *packets.SubscribePacket:
		level = CheckTopicsForPoolLevel(ca.(*packets.SubscribePacket).Topics)
	case *packets.UnsubscribePacket:
		level = CheckTopicsForPoolLevel(ca.(*packets.UnsubscribePacket).Topics)
	default:
		level = msg.client.info.msgLevel
	}
	return level
}

func NewBroker(config *Config) (*Broker, error) {
	b := &Broker{
		id:       GenUniqueId(),
		config:   config,
		mfwPools: pool.NewMultiFixedWorkPools(uint16(config.RegularWorker), uint16(config.SpecialWorker), uint16(config.SupremeWorker)),
	}

	var err error
	b.topicsMgr, err = topics.NewManager("mem")
	if err != nil {
		log.Error("new topic manager error", zap.Error(err))
		return nil, err
	}

	b.sessionMgr, err = sessions.NewManager("mem")
	if err != nil {
		log.Error("new session manager error", zap.Error(err))
		return nil, err
	}

	if b.config.TlsPort != "" {
		tlsConfig, err := NewTLSConfig(b.config.TlsInfo)
		if err != nil {
			log.Error("new tlsConfig error", zap.Error(err))
			return nil, err
		}
		b.tlsConfig = tlsConfig
	}

	if b.config.Acl {
		aclconfig, err := acl.AclConfigLoad(b.config.AclConf)
		if err != nil {
			log.Error("Load acl conf error", zap.Error(err))
			return nil, err
		}
		b.AclConfig = aclconfig
		b.StartAclWatcher()
	}

	return b, nil
}

func (b *Broker) SubmitWork(clientId string, msg *Message) {
	if b.mfwPools == nil {
		b.mfwPools = pool.NewMultiFixedWorkPools(uint16(b.config.RegularWorker), uint16(b.config.SpecialWorker), uint16(b.config.SupremeWorker))
	}

	level := b.GetTaskPoolLevel(msg)
	if !ValidPoolLevel(level) {
		level = RegularLevel
		log.Warn("broker/SubmitWork: Invalid Message Level, Reset it to Regular Level")
	}
	b.mfwPools.SubmitTaskWithType(level, clientId, func() {
		ProcessMessage(msg)
	})

}

func (b *Broker) Start() {
	if b == nil {
		log.Error("broker is null")
		return
	}

	//go InitHTTPMonitor(b)

	//listen client over tcp
	if b.config.Port != "" {
		go b.StartClientListening(false)
	}

	//listen for websocket
	log.Info("Websocket Configure", zap.String("WsPath", b.config.WsPath), zap.String("WsPort", b.config.WsPort))
	if b.config.WsPort != "" {
		go b.StartWebsocketListening()
	}

	//listen client over tls
	if b.config.TlsPort != "" {
		go b.StartClientListening(true)
	}

	//system monitor
	go b.State()

	//Pool Metrics and Stat
	go b.PoolState()

	//Pool Stats' Stat
	b.StatInfoStat()
}

func (b *Broker) State() {
	v, _ := mem.VirtualMemory()
	timeSticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-timeSticker.C:
			/*if v.UsedPercent > 80 {
				debug.FreeOSMemory()
			}*/
			b.StateNotification(v)
		}
	}
}

func (b *Broker) StateNotification(vms *mem.VirtualMemoryStat) {
	vmUsedSize := float32(vms.Used) / 1024.0 / 1024.0
	info := fmt.Sprintf(`{"memory used percent (%%)":%.2f,"memery used (MB)":%.2f}`, vms.UsedPercent, vmUsedSize)
	b.StringNotification("$SYS/Broker/Memory", info)
}

func (b *Broker) PoolState() {
	timeSticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-timeSticker.C:
			m, h, d, a := b.mfwPools.RegularFixedWorkPool.ReportStreamTopK()
			b.StringNotification("$SYS/Broker/Stream/TopK/Regular/Last_Minute", m)
			b.StringNotification("$SYS/Broker/Stream/TopK/Regular/Last_Hour", h)
			b.StringNotification("$SYS/Broker/Stream/TopK/Regular/Last_Day", d)
			b.StringNotification("$SYS/Broker/Stream/TopK/Regular/Online_Period", a)

			m, h, d, a = b.mfwPools.SpecialFixedWorkPool.ReportStreamTopK()
			b.StringNotification("$SYS/Broker/Stream/TopK/Special/Last_Minute", m)
			b.StringNotification("$SYS/Broker/Stream/TopK/Special/Last_Hour", h)
			b.StringNotification("$SYS/Broker/Stream/TopK/Special/Last_Day", d)
			b.StringNotification("$SYS/Broker/Stream/TopK/Special/Online_Period", a)

			m, h, d, a = b.mfwPools.SupremeFixedWorkPool.ReportStreamTopK()
			b.StringNotification("$SYS/Broker/Stream/TopK/Supreme/Last_Minute", m)
			b.StringNotification("$SYS/Broker/Stream/TopK/Supreme/Last_Hour", h)
			b.StringNotification("$SYS/Broker/Stream/TopK/Supreme/Last_Day", d)
			b.StringNotification("$SYS/Broker/Stream/TopK/Supreme/Online_Period", a)

			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Regular", b.mfwPools.RegularFixedWorkPool.ReportTaskQueueMetricsWithJson())
			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Special", b.mfwPools.SpecialFixedWorkPool.ReportTaskQueueMetricsWithJson())
			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Supreme", b.mfwPools.SupremeFixedWorkPool.ReportTaskQueueMetricsWithJson())

			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Regular/Worker", b.mfwPools.RegularFixedWorkPool.ReportTaskQueueMetricsByWorkerWithJson())
			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Special/Worker", b.mfwPools.SpecialFixedWorkPool.ReportTaskQueueMetricsByWorkerWithJson())
			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Supreme/Worker", b.mfwPools.SupremeFixedWorkPool.ReportTaskQueueMetricsByWorkerWithJson())

			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Regular/Worker/Status_Stat", b.mfwPools.RegularFixedWorkPool.ReportTaskQueueWorkerStatusStatInfoWithJson())
			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Special/Worker/Status_Stat", b.mfwPools.SpecialFixedWorkPool.ReportTaskQueueWorkerStatusStatInfoWithJson())
			b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Supreme/Worker/Status_Stat", b.mfwPools.SupremeFixedWorkPool.ReportTaskQueueWorkerStatusStatInfoWithJson())
		}
	}
}

func (b *Broker) StatInfoStat() {
	go func() {
		for {
			select {
			case info := <-*b.mfwPools.RegularFixedWorkPool.GetTaskQueueStatsForWorkerStatusStatsJson():
				b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Regular/Worker/Status_Stat/Stat", info)
			}
		}
	}()
	go func() {
		for {
			select {
			case info := <-*b.mfwPools.SpecialFixedWorkPool.GetTaskQueueStatsForWorkerStatusStatsJson():
				b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Special/Worker/Status_Stat/Stat", info)
			}
		}
	}()
	go func() {
		for {
			select {
			case info := <-*b.mfwPools.SupremeFixedWorkPool.GetTaskQueueStatsForWorkerStatusStatsJson():
				b.StringNotification("$SYS/Broker/Task_Queue/Metrics/Supreme/Worker/Status_Stat/Stat", info)
			}
		}
	}()
}

func (b *Broker) StringNotification(topic string, info string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = topic
	packet.Qos = 0
	packet.Payload = []byte(info)

	b.PublishMessage(packet)
}

func (b *Broker) StartWebsocketListening() {
	hp := b.config.Host + ":" + b.config.WsPort
	path := b.config.WsPath

	log.Info("Start Websocket Listener On", zap.String("hp", hp), zap.String("path", path))
	http.Handle(path, websocket.Handler(b.wsHandler))
	var err error
	if b.config.WsTLS {
		err = http.ListenAndServeTLS(hp, b.config.TlsInfo.CertFile, b.config.TlsInfo.KeyFile, nil)
	} else {
		err = http.ListenAndServe(hp, nil)
	}
	if err != nil {
		log.Error("broker/StartWebsocketListening: Error Listen And Serve On " + err.Error())
		return
	}
}

func (b *Broker) wsHandler(ws *websocket.Conn) {
	// io.Copy(ws, ws)
	ws.PayloadType = websocket.BinaryFrame
	b.handleConnection(ws)
}

func (b *Broker) StartClientListening(Tls bool) {
	var hp string
	var err error
	var l net.Listener
	if Tls {
		hp = b.config.TlsHost + ":" + b.config.TlsPort
		l, err = tls.Listen("tcp", hp, b.tlsConfig)
		log.Info("Start TLS Listening Client On ", zap.String("hp", hp))
	} else {
		hp := b.config.Host + ":" + b.config.Port
		l, err = net.Listen("tcp", hp)
		log.Info("Start Listening Client On ", zap.String("hp", hp))
	}
	if err != nil {
		log.Error("broker/StartClientListening: Error listening on ", zap.Error(err))
		return
	}
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("broker/StartClientListening: Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne), zap.Duration("sleeping", tmpDelay/time.Millisecond))
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("broker/StartClientListening: Accept error=> %v", zap.Error(err))
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		go b.handleConnection(conn)
	}
}

func (b *Broker) Handshake(conn net.Conn) bool {

	nc := tls.Server(conn, b.tlsConfig)
	time.AfterFunc(DEFAULT_TLS_TIMEOUT, func() { TlsTimeout(nc) })
	_ = nc.SetReadDeadline(time.Now().Add(DEFAULT_TLS_TIMEOUT))

	// Force handshake
	if err := nc.Handshake(); err != nil {
		log.Error("TLS handshake error, ", zap.Error(err))
		return false
	}
	_ = nc.SetReadDeadline(time.Time{})
	return true

}

func TlsTimeout(conn *tls.Conn) {
	nc := conn
	// Check if already closed
	if nc == nil {
		return
	}
	cs := nc.ConnectionState()
	if !cs.HandshakeComplete {
		log.Error("TLS handshake timeout")
		_ = nc.Close()
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	//process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		log.Error("broker/handleConnection: read connect packet error => ", zap.Error(err))
		return
	}
	if packet == nil {
		log.Error("broker/handleConnection: received nil packet")
		return
	}
	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		log.Error("broker/handleConnection: received msg that was not Connect")
		return
	}

	log.Info("read connect from ", zap.String("clientID", msg.ClientIdentifier))

	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.SessionPresent = msg.CleanSession
	connack.ReturnCode = msg.Validate()

	if connack.ReturnCode != packets.Accepted {
		err = connack.Write(conn)
		if err != nil {
			log.Error("broker/handleConnection: send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
			return
		}
		return
	}
	/*
		if !b.CheckConnectAuth(string(msg.ClientIdentifier), string(msg.Username), string(msg.Password)) {
			connack.ReturnCode = packets.ErrRefusedNotAuthorised
			err = connack.Write(conn)
			if err != nil {
				log.Error("broker/handleConnection: send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
				return
			}
			return
		}
	*/

	err = connack.Write(conn)
	if err != nil {
		log.Error("broker/handleConnection: send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
		return
	}
	/*
		b.Publish(&plugins.Elements{
				ClientID:  string(msg.ClientIdentifier),
				Username:  string(msg.Username),
				Action:    plugins.Connect,
				Timestamp: time.Now().Unix(),
		})*/

	var willMsg *packets.PublishPacket
	if msg.WillFlag {
		willMsg = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		willMsg.Qos = msg.WillQos
		willMsg.TopicName = msg.WillTopic
		willMsg.Retain = msg.WillRetain
		willMsg.Payload = msg.WillMessage
		willMsg.Dup = msg.Dup
	} else {
		willMsg = nil
	}
	info := info{
		clientID:  msg.ClientIdentifier,
		username:  msg.Username,
		password:  msg.Password,
		keepalive: msg.Keepalive,
		willMsg:   willMsg,
	}

	c := &client{
		broker: b,
		conn:   conn,
		info:   info,
	}

	c.init()

	err = b.getSession(c, msg, connack)
	if err != nil {
		log.Error("broker/handleConnection: get session error => ", zap.String("clientID", c.info.clientID))
		return
	}

	cid := c.info.clientID

	var exist bool
	var old interface{}

	old, exist = b.clients.Load(cid)
	if exist {
		log.Warn("broker/handleConnection: client exist, close old ...", zap.String("clientID", c.info.clientID))
		ol, ok := old.(*client)
		if ok {
			ol.Close()
		}
	}
	b.clients.Store(cid, c)
	go b.OnlineOfflineNotification(cid, true)

	c.readLoop()
}

func (b *Broker) removeClient(c *client) {
	clientId := c.info.clientID
	b.clients.Delete(clientId)
	log.Info("broker/removeClient: delete client ,", zap.String("ClientId", clientId))
}

func (b *Broker) PublishMessage(packet *packets.PublishPacket) {
	var subs []interface{}
	var qoss []byte
	b.mu.Lock()
	err := b.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &subs, &qoss)
	b.mu.Unlock()
	if err != nil {
		log.Error("broker/PublishMessage: search sub client error.", zap.Error(err))
		return
	}

	for _, sub := range subs {
		s, ok := sub.(*subscription)
		if ok {
			err := s.client.WriterPacket(packet)
			if err != nil {
				log.Error("broker/PublishMessage: write message error.", zap.Error(err))
			}
		}
	}
}

func (b *Broker) OnlineOfflineNotification(clientID string, online bool) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/Broker/Connection/Clients/" + clientID
	packet.Qos = 0
	packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, clientID, online, time.Now().UTC().Format(time.RFC3339)))

	b.PublishMessage(packet)
}
