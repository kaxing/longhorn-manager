package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type HousekeepingState string

const (
	HousekeepingStateNew        = HousekeepingState("")
	HousekeepingStateInProgress = HousekeepingState("InProgress")
	HousekeepingStateCompleted  = HousekeepingState("Completed")
	HousekeepingStateError      = HousekeepingState("Error")
	HousekeepingStateUnknown    = HousekeepingState("Unknown")
)

// HousekeepingSpec defines the desired state of the Longhorn houskeeping
type HousekeepingSpec struct {
	// The time to request run houskeeping.
	// +optional
	// +nullable
	SyncRequestedAt metav1.Time `json:"syncRequestedAt"`
}

// HousekeepingStatus defines the observed state of the Longhorn housekeepling
type HousekeepingStatus struct {
	// The node ID on which the controller is responsible to reconcile this housekeepling CR.
	// +optional
	OwnerID string `json:"ownerID"`
	// The housekeepling creation state.
	// Can be "", "InProgress", "Completed", "Error", "Unknown".
	// +optional
	State HousekeepingState `json:"state"`
	// The housekeepling finished time.
	// +optional
	HousekeepingCreatedAt string `json:"housekeepingCreatedAt"`
	// The last time that the housekeepling finished.
	// +optional
	// +nullable
	LastSyncedAt metav1.Time `json:"lastSyncedAt"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhh
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The housekeepling state"
// +kubebuilder:printcolumn:name="LastSyncedAt",type=string,JSONPath=`.status.lastSyncedAt`,description="The last time that the housekeepling finished"

// Housekeeping is where Longhorn stores housekeeping object.
type Housekeeping struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HousekeepingSpec   `json:"spec,omitempty"`
	Status HousekeepingStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HousekeepingList is a list of housekeepings.
type HousekeepingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Housekeeping `json:"items"`
}
