package paxos

import (
	"sync"
	"testing"
)

func TestNewAcceptor(t *testing.T) {
	prepareReqChan := make(chan PrepareRequest, 1)
	acceptReqChan := make(chan AcceptRequest, 1)
	var wg sync.WaitGroup

	a := NewAcceptor("foo-acceptor", prepareReqChan, acceptReqChan, &wg)

	if a.name != "foo-acceptor" {
		t.Errorf("Acceptor's name does not have a correct value: %+v", a)
	}

	if a.prepareReqChan != prepareReqChan {
		t.Errorf("Acceptor's prepareReqChan does not have a correct value: %+v", a)
	}

	if a.acceptReqChan != acceptReqChan {
		t.Errorf("Acceptor's acceptReqChan does not have a correct value: %+v", a)
	}

	if a.waitGroup != &wg {
		t.Errorf("Acceptor's waitGroup odoes not have a correct value: %+v", a)
	}
}
