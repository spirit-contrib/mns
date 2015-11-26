package mns

type MNSFlowMetadata struct {
	CurrentFlowId int      `json:"current_flow_id"`
	Error         []string `json:"error"`
	Normal        []string `json:"normal"`
}

type MNSParallelFlowMetadata struct {
	Id    string `json:"id"`
	Index int    `json:"index"`
	Count int    `json:"count"`
}
