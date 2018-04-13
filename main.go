package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/josephjang/single-decree-paxos-go/paxos"

	p "github.com/josephjang/single-decree-paxos-go/paxos"
	log "github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

const channelBufferSize = 1024

func init() {
	log.SetFormatter(&prefixed.TextFormatter{})
	log.SetLevel(log.InfoLevel)
}

// send prepare requests to acceptors
func proposer(name string, num int64, val int64,
	prepareReqChans []chan p.PrepareRequest,
	acceptReqChans []chan p.AcceptRequest,
	wg *sync.WaitGroup) {
	p := paxos.NewProposer(name, num, val, prepareReqChans, acceptReqChans, wg)
	p.Run()
}

var shutdown = false

// process prepare/accept requests
func acceptor(name string, prepareReqChan chan p.PrepareRequest,
	acceptReqChan chan p.AcceptRequest,
	wg *sync.WaitGroup) {

	defer wg.Done()

	// TODO: ID
	log.Infof("Acceptor %s: Started", name)

	var maxProposalNumber int64 = -1
	var acceptedProposal = p.Proposal{
		ProposalNumber: -1,
	}

	acceptorTimeout := 10 * time.Second

	for !shutdown {
		select {
		case prepareReq := <-prepareReqChan:
			log.WithField("number", prepareReq.ProposalNumber).Infof("Acceptor %s: Received a prepare request", name)
			if prepareReq.ProposalNumber > maxProposalNumber {
				maxProposalNumber = prepareReq.ProposalNumber
				// send response
				prepareResp := p.PrepareResponse{
					ProposalNumber:   prepareReq.ProposalNumber,
					AcceptedProposal: acceptedProposal,
				}
				// TODO: send async
				prepareReq.PrepareResponseChan <- prepareResp
				log.WithFields(log.Fields{
					"number":          prepareResp.ProposalNumber,
					"accepted-number": prepareResp.AcceptedProposal.ProposalNumber,
					"accepted-value":  prepareResp.AcceptedProposal.Value,
				}).Infof("Acceptor %s: Sent a prepare response", name)

			} else {
				log.WithField("number", prepareReq.ProposalNumber).Infof("Acceptor %s: Ignore the Prepare request", name)
			}
		case acceptReq := <-acceptReqChan:
			log.WithFields(log.Fields{
				"number": acceptReq.ProposalNumber,
				"value":  acceptReq.Value,
			}).Infof("Acceptor %s: Received an accept request", name)
			if acceptReq.ProposalNumber >= maxProposalNumber {
				maxProposalNumber = acceptReq.ProposalNumber
				acceptedProposal = p.Proposal{
					ProposalNumber: acceptReq.ProposalNumber,
					Value:          acceptReq.Value,
				}
				acceptorTimeout = 3 * time.Second // reduce the timeout since there is the accepted proposal
				log.Infof("Acceptor %s: Updated the accepted proposal: %+v", name, acceptedProposal)
			} else {
				log.WithFields(log.Fields{
					"number": acceptReq.ProposalNumber,
					"value":  acceptReq.Value,
				}).Infof("Acceptor %s: Ignored an accept request: %+v", name, acceptReq)
			}
			// TODO: write the value
		case <-time.After(acceptorTimeout):
			log.Infof("Acceptor %s: Timeout while waiting for messages", name)
			log.Infof("Acceptor %s: Terminated", name)
			return
		}
	}

	log.Infof("Acceptor %s: Shutdowned", name)
}

func main() {
	// channels for acceptors
	var prepareReqChans = []chan p.PrepareRequest{
		make(chan p.PrepareRequest, channelBufferSize),
		make(chan p.PrepareRequest, channelBufferSize),
		make(chan p.PrepareRequest, channelBufferSize),
	}

	var acceptReqChans = []chan p.AcceptRequest{
		make(chan p.AcceptRequest, channelBufferSize),
		make(chan p.AcceptRequest, channelBufferSize),
		make(chan p.AcceptRequest, channelBufferSize),
	}

	// channels for proposers

	var proposerWaitGroup sync.WaitGroup
	var acceptorWaitGroup sync.WaitGroup

	for i := range prepareReqChans {
		acceptorWaitGroup.Add(1)
		go acceptor(fmt.Sprintf("A%d", i+1), prepareReqChans[i], acceptReqChans[i], &acceptorWaitGroup)
	}

	proposerWaitGroup.Add(3)
	go proposer("P1", 1, 41, prepareReqChans, acceptReqChans, &proposerWaitGroup)
	go proposer("P2", 2, 42, prepareReqChans, acceptReqChans, &proposerWaitGroup)

	time.Sleep(1 * time.Second)
	go proposer("P3", 3, 10, prepareReqChans, acceptReqChans, &proposerWaitGroup)

	proposerWaitGroup.Wait()
	log.Infof("System: All proposers are finished")
	acceptorWaitGroup.Wait()
	log.Infof("System: All acceptors are finished")
}
