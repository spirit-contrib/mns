package mns

type MNSFlowMetadata struct {
	CurrentFlowId int64    `json:"current_flow_id"`
	Error         []string `json:"error"`
	Normal        []string `json:"normal"`
}
