package requests

type Text struct {
	LocalSubRegion     	string  `json:"LocalSubRegion"`
	LocalRegion     	string  `json:"LocalRegion"`
	Country				string	`json:"Country"`
	Language          	string  `json:"Language"`
	LocalSubRegionName	string  `json:"LocalSubRegionName"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
