package paxos

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

/*
Acceptor represents a acceptor in single-decree Paxos protocol

	- name: name of the acceptor
	- prepareReqChan: a channel to receive PrepareRequest (p1a)
	- acceptReqChan: a channel to receive AcceptRequest (p2a)
	- waitGroup: wait group to notify the proposer process is finished
*/
type Acceptor struct {
	name           string
	prepareReqChan chan PrepareRequest
	acceptReqChan  chan AcceptRequest
	waitGroup      *sync.WaitGroup
}

//
// NewAcceptor creates a new instance of Acceptor
//
func NewAcceptor(name string, prepareReqChan chan PrepareRequest, acceptReqChan chan AcceptRequest, wg *sync.WaitGroup) *Acceptor {
	a := &Acceptor{
		name:           name,
		prepareReqChan: prepareReqChan,
		acceptReqChan:  acceptReqChan,
		waitGroup:      wg,
	}
	return a
}

//
// Run execute the acceptor's process
//
func (a *Acceptor) Run() {
	var shutdown = false

	defer a.waitGroup.Done()

	// TODO: ID
	log.Infof("Acceptor %s: Started", a.name)

	var maxProposalNumber int64 = -1
	var acceptedProposal = Proposal{
		ProposalNumber: -1,
	}

	acceptorTimeout := 10 * time.Second

	for !shutdown {
		select {
		case prepareReq := <-a.prepareReqChan:
			log.WithField("number", prepareReq.ProposalNumber).Infof("Acceptor %s: Received a prepare request", a.name)
			if prepareReq.ProposalNumber > maxProposalNumber {
				maxProposalNumber = prepareReq.ProposalNumber
				// send response
				prepareResp := PrepareResponse{
					ProposalNumber:   prepareReq.ProposalNumber,
					AcceptedProposal: acceptedProposal,
				}
				// TODO: send async
				prepareReq.PrepareResponseChan <- prepareResp
				log.WithFields(log.Fields{
					"number":          prepareResp.ProposalNumber,
					"accepted-number": prepareResp.AcceptedProposal.ProposalNumber,
					"accepted-value":  prepareResp.AcceptedProposal.Value,
				}).Infof("Acceptor %s: Sent a prepare response", a.name)

			} else {
				log.WithField("number", prepareReq.ProposalNumber).Infof("Acceptor %s: Ignore the Prepare request", a.name)
			}
		case acceptReq := <-a.acceptReqChan:
			log.WithFields(log.Fields{
				"number": acceptReq.ProposalNumber,
				"value":  acceptReq.Value,
			}).Infof("Acceptor %s: Received an accept request", a.name)
			if acceptReq.ProposalNumber >= maxProposalNumber {
				maxProposalNumber = acceptReq.ProposalNumber
				acceptedProposal = Proposal{
					ProposalNumber: acceptReq.ProposalNumber,
					Value:          acceptReq.Value,
				}
				acceptorTimeout = 3 * time.Second // reduce the timeout since there is the accepted proposal
				log.Infof("Acceptor %s: Updated the accepted proposal: %+v", a.name, acceptedProposal)
			} else {
				log.WithFields(log.Fields{
					"number": acceptReq.ProposalNumber,
					"value":  acceptReq.Value,
				}).Infof("Acceptor %s: Ignored an accept request: %+v", a.name, acceptReq)
			}
			// TODO: write the value
		case <-time.After(acceptorTimeout):
			log.Infof("Acceptor %s: Timeout while waiting for messages", a.name)
			log.Infof("Acceptor %s: Terminated", a.name)
			return
		}
	}

	log.Infof("Acceptor %s: Shutdowned", a.name)
}
