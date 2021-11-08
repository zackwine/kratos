package kratos

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/websocket"
	"github.com/xmidt-org/webpa-common/logging"
	"github.com/xmidt-org/wrp-go/v3"
)

// Client is what function calls we expose to the user of kratos
type Client interface {
	Hostname() string
	HandlerRegistry() HandlerRegistry
	Send(message *wrp.Message)
	Close() error
}

// sendWRPFunc is the function for sending a message downstream.
type sendWRPFunc func(*wrp.Message)

type client struct {
	deviceID        string
	userAgent       string
	deviceProtocols string
	hostname        string
	registry        HandlerRegistry
	handlePingMiss  HandlePingMiss
	encoderSender   encoderSender
	decoderSender   decoderSender
	connection      websocketConnection
	headerInfo      *clientHeader
	logger          log.Logger
	done            chan struct{}
	wg              sync.WaitGroup
	pingConfig      PingConfig
	once            sync.Once
	config          ClientConfig
	connected       bool
	connectedMutex  sync.Mutex
}

// used to track everything that we want to know about the client headers
type clientHeader struct {
	deviceName   string
	firmwareName string
	modelName    string
	manufacturer string
	token        string
}

// websocketConnection maintains the websocket connection upstream (to XMiDT).
type websocketConnection interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

// Hostname provides the client's hostname.
func (c *client) Hostname() string {
	return c.hostname
}

// HandlerRegistry returns the HandlerRegistry that the client maintains.
func (c *client) HandlerRegistry() HandlerRegistry {
	return c.registry
}

// Send is used to open a channel for writing to XMiDT
func (c *client) Send(message *wrp.Message) {
	if !c.isConnected() {
		logging.Warn(c.logger).Log(logging.MessageKey(), "Send failed because connection is closed...")
		return
	}
	c.encoderSender.EncodeAndSend(message)
}

// Close closes connections downstream and the socket upstream.
func (c *client) Close() error {
	var connectionErr error
	c.once.Do(func() {
		logging.Info(c.logger).Log(logging.MessageKey(), "Closing client...")
		close(c.done)
		c.wg.Wait()
		c.decoderSender.Close()
		c.encoderSender.Close()
		connectionErr = c.connection.Close()
		c.connection = nil
		// TODO: if this fails, can we really do anything. Is there potential for leaks?
		// if err != nil {
		// 	return emperror.Wrap(err, "Failed to close connection")
		// }
		logging.Info(c.logger).Log(logging.MessageKey(), "Client Closed")
	})
	return connectionErr
}

func (c *client) setConnected() {
	c.connectedMutex.Lock()
	c.connected = true
	c.connectedMutex.Unlock()
}

func (c *client) RequestReconnect() {
	logging.Info(c.logger).Log(logging.MessageKey(), "Client RequestReconnect...")
	c.connectedMutex.Lock()
	c.connected = false
	if c.connection != nil {
		c.connection.Close()
		c.encoderSender.Close()
		c.connection = nil
		c.encoderSender = nil
	}
	c.connectedMutex.Unlock()
}

func (c *client) isConnected() (connected bool) {
	c.connectedMutex.Lock()
	connected = c.connected
	c.connectedMutex.Unlock()
	return connected
}

// going to be used to access the HandleMessage() function
func (c *client) read() {
	defer c.wg.Done()
	logging.Info(c.logger).Log(logging.MessageKey(), "Watching socket for messages.")

	for {
		select {
		case <-c.done:
			logging.Info(c.logger).Log(logging.MessageKey(), "Stopped reading from socket.")
			return
		default:
			if !c.isConnected() {
				c.reconnect()
				continue
			}

			logging.Debug(c.logger).Log(logging.MessageKey(), "Reading message...")
			mt, serverMessage, err := c.connection.ReadMessage()
			if mt == websocket.CloseMessage {
				logging.Info(c.logger).Log(logging.MessageKey(), "Received close message type.")
				c.RequestReconnect()
				continue
			}
			logging.Info(c.logger).Log(logging.MessageKey(), "Read Message")

			if err != nil {
				logging.Info(c.logger).Log(logging.MessageKey(), "Failed to read message.", logging.ErrorKey(), err.Error())
				c.RequestReconnect()
				continue
			}

			c.decoderSender.DecodeAndSend(serverMessage)

			logging.Debug(c.logger).Log(logging.MessageKey(), "Message sent to be decoded")
		}
	}
}

func (c *client) reconnect() error {

	config := c.config
	inHeader := &clientHeader{
		deviceName:   config.DeviceName,
		firmwareName: config.FirmwareName,
		modelName:    config.ModelName,
		manufacturer: config.Manufacturer,
		token:        config.Token,
	}

	logging.Warn(c.logger).Log(logging.MessageKey(), "Reconnecting...")
	newConnection, _, err := createConnection(inHeader, config.DestinationURL, config.TlsConfig)

	if err != nil {
		logging.Debug(c.logger).Log(logging.MessageKey(), "Failed to connect, retrying after a timeout", "error", err)
		<-time.After(time.Second * 2)
		return err
	}
	logging.Warn(c.logger).Log(logging.MessageKey(), "Reconnected successfully...")

	pinged := make(chan string)
	newConnection.SetPingHandler(func(appData string) error {
		pinged <- appData
		err := newConnection.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
		return err
	})
	var logger log.Logger
	if config.ClientLogger != nil {
		logger = config.ClientLogger
	} else {
		logger = logging.DefaultLogger()
	}
	sender := NewSender(newConnection, config.OutboundQueue.MaxWorkers, config.OutboundQueue.Size, logger)
	encoder := NewEncoderSender(sender, config.WRPEncoderQueue.MaxWorkers, config.WRPEncoderQueue.Size, logger)

	c.connection = newConnection
	c.encoderSender = encoder

	c.setConnected()

	return nil
}
