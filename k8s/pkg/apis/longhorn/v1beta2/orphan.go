package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type OrphanType string

const (
	OrphanTypeNew = OrphanType("replica")
)

const (
	OrphanConditionTypeDeletable = "Deletable"
)

// OrphanSpec defines the desired state of the Longhorn orphaned data
type OrphanSpec struct {
	// The node ID on which the controller is responsible to reconcile this orphan CR.
	// +optional
	NodeID string `json:"nodeID"`
	// The type of the orphaned data.
	// Can be "replica".
	// +optional
	Type OrphanType `json:"type"`

	// The directory or file name of the orphaned data
	// +optional
	Name string `json:"fileName"`

	// +optional
	DiskUUID string `json:"diskUUID"`
	// +optional
	DiskPath string `json:"diskPath"`
}

// OrphanStatus defines the observed state of the Longhorn orphaned data
type OrphanStatus struct {
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lho
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`,description="The type of the orphan"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node that the orphan is on"
// +kubebuilder:printcolumn:name="File",type=string,JSONPath=`.spec.fileName`,description="The current file or directory name of the orphan"
// +kubebuilder:printcolumn:name="Disk",type=string,JSONPath=`.spec.diskPath`,description="The disk that the orphan is on"

// Orphan is where Longhorn stores orphan object.
type Orphan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OrphanSpec   `json:"spec,omitempty"`
	Status OrphanStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OrphanList is a list of orphans.
type OrphanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Orphan `json:"items"`
}
