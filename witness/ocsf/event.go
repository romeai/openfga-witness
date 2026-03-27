package ocsf

const (
	ClassUIDAPIActivity    = 6003
	CategoryUIDApplication = 6

	ActivityIDCreate = 1
	ActivityIDRead   = 2
	ActivityIDUpdate = 3
	ActivityIDDelete = 4
	ActivityIDOther  = 99

	SeverityIDInformational = 1
	SeverityIDLow           = 2
	SeverityIDMedium        = 3

	StatusIDSuccess = 1
	StatusIDFailure = 2

	DispositionIDAllowed = 1
	DispositionIDBlocked = 2
)

type APIActivityEvent struct {
	ClassUID       int              `json:"class_uid"`
	CategoryUID    int              `json:"category_uid"`
	ActivityID     int              `json:"activity_id"`
	ActivityName   string           `json:"activity_name"`
	TypeUID        int              `json:"type_uid"`
	TypeName       string           `json:"type_name"`
	Time           int64            `json:"time"`
	SeverityID     int              `json:"severity_id"`
	Severity       string           `json:"severity"`
	StatusID       int              `json:"status_id"`
	Status         string           `json:"status"`
	StatusCode     string           `json:"status_code,omitempty"`
	StatusDetail   string           `json:"status_detail,omitempty"`
	Message        string           `json:"message,omitempty"`
	Duration       int64            `json:"duration"`
	Metadata       Metadata         `json:"metadata"`
	Actor          Actor            `json:"actor"`
	Api            Api              `json:"api"`
	SrcEndpoint    SrcEndpoint      `json:"src_endpoint"`
	Resources      []Resource       `json:"resources,omitempty"`
	Authorizations []Authorization  `json:"authorizations,omitempty"`
	DispositionID  *int             `json:"disposition_id,omitempty"`
	Disposition    string           `json:"disposition,omitempty"`
	Unmapped       map[string]any   `json:"unmapped,omitempty"`
}

type Metadata struct {
	Version   string  `json:"version"`
	Product   Product `json:"product"`
	UID       string  `json:"uid,omitempty"`
	TenantUID string  `json:"tenant_uid,omitempty"`
}

type Product struct {
	Name       string `json:"name"`
	VendorName string `json:"vendor_name"`
	Version    string `json:"version,omitempty"`
}

type Actor struct {
	User   ActorUser `json:"user"`
	AppUID string    `json:"app_uid,omitempty"`
}

type ActorUser struct {
	UID    string `json:"uid,omitempty"`
	TypeID int    `json:"type_id,omitempty"`
	Type   string `json:"type,omitempty"`
}

type Api struct {
	Operation string      `json:"operation"`
	Service   ServiceInfo `json:"service"`
	Version   string      `json:"version,omitempty"`
}

type ServiceInfo struct {
	Name string `json:"name"`
}

type SrcEndpoint struct {
	IP   string `json:"ip,omitempty"`
	Port int    `json:"port,omitempty"`
}

type Resource struct {
	Name string         `json:"name,omitempty"`
	Type string         `json:"type,omitempty"`
	UID  string         `json:"uid,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

type Authorization struct {
	Decision string  `json:"decision,omitempty"`
	Policy   *Policy `json:"policy,omitempty"`
}

type Policy struct {
	Name string `json:"name,omitempty"`
	UID  string `json:"uid,omitempty"`
}
