package sessions

import (
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/require"
)

func TestSessionInit(t *testing.T) {
	sess := &Session{}
	cmsg := newConnectMessage()

	err := sess.Init(cmsg)
	require.NoError(t, err)
	require.Equal(t, true, sess.initted)
	require.Equal(t, cmsg.WillQos, sess.cmsg.WillQos)
	require.Equal(t, cmsg.ProtocolVersion, sess.cmsg.ProtocolVersion)
	require.Equal(t, cmsg.CleanSession, sess.cmsg.CleanSession)
	require.Equal(t, cmsg.ClientIdentifier, sess.cmsg.ClientIdentifier)
	require.Equal(t, cmsg.Keepalive, sess.cmsg.Keepalive)
	require.Equal(t, cmsg.WillTopic, sess.cmsg.WillTopic)
	require.Equal(t, cmsg.WillMessage, sess.cmsg.WillMessage)
	require.Equal(t, cmsg.Username, sess.cmsg.Username)
	require.Equal(t, cmsg.Password, sess.cmsg.Password)
	require.Equal(t, "will", sess.cmsg.WillTopic)
	require.Equal(t, cmsg.WillQos, sess.cmsg.WillQos)

	err = sess.AddTopic("test", 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(sess.topics))

	topics, qoss, err := sess.Topics()
	require.NoError(t, err)
	require.Equal(t, 1, len(topics))
	require.Equal(t, 1, len(qoss))
	require.Equal(t, "test", topics[0])
	require.Equal(t, 1, int(qoss[0]))

	err = sess.RemoveTopic("test")
	require.NoError(t, err)
	require.Equal(t, 0, len(sess.topics))
}

func TestSessionRetainMessage(t *testing.T) {
	sess := &Session{}
	cmsg := newConnectMessage()
	err := sess.Init(cmsg)
	require.NoError(t, err)

	msg := newPublishMessage(1234, 1)
	err = sess.RetainMessage(msg)
	require.NoError(t, err)
	require.Equal(t, sess.Retained, msg)
}

func newConnectMessage() *packets.ConnectPacket {
	msg := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	msg.WillQos = 1
	msg.ProtocolVersion = 4
	msg.CleanSession = true
	msg.ClientIdentifier = "Mqtt-Broker-Single-Node-ID"
	msg.Keepalive = 10
	msg.WillTopic = "will"
	msg.WillMessage = []byte("send me home")
	msg.Username = "MQTT-Broker-SN"
	msg.Password = []byte("VerySecret")

	return msg
}

func newPublishMessage(pktid uint16, qos byte) *packets.PublishPacket {
	msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	msg.MessageID = pktid
	msg.Qos = qos
	msg.TopicName = "abc"
	msg.Payload = []byte("abc")
	return msg
}
