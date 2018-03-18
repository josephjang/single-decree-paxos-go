package paxos

// PrepareRequest is a message sent by a proposer to acceptors in the Prepare Phasehhh
type PrepareRequest struct {
	ProposalNumber      int64
	PrepareResponseChan chan PrepareResponse
}

// PrepareResponse is a message sent by acceptors to the proposer
// in response to PrepareRequest
type PrepareResponse struct {
	ProposalNumber   int64
	AcceptedProposal Proposal
}

// Proposal is the Value associated with the ProposalNumber
type Proposal struct {
	ProposalNumber int64
	Value          int64
}

// AcceptRequest is a message sent by the proposer to acceptors in the Accept Phase
type AcceptRequest struct {
	ProposalNumber int64
	Value          int64
}
