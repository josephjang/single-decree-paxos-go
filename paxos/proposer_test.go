package paxos

import (
	"sync"
	"testing"
)

func TestNewProposer(t *testing.T) {
	prepareChans := []chan PrepareRequest{
		make(chan PrepareRequest, 1),
	}
	acceptChans := []chan AcceptRequest{
		make(chan AcceptRequest, 1),
	}

	var wg sync.WaitGroup

	p := NewProposer("foo-proposer", 1, 42, prepareChans, acceptChans, &wg)

	if p.val != 42 {
		t.Errorf("Proposer's Val does not have a correct value: %+v", p)
	}

	// TODO: more validation
}
