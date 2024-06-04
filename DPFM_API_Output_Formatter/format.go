package dpfm_api_output_formatter

import (
	"data-platform-api-local-sub-region-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToLocalSubRegion(rows *sql.Rows) (*[]LocalSubRegion, error) {
	defer rows.Close()
	localSubRegion := make([]LocalSubRegion, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.LocalSubRegion{}

		err := rows.Scan(
			&pm.LocalSubRegion,
			&pm.LocalRegion,
			&pm.Country,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &localSubRegion, nil
		}

		data := pm
		localSubRegion = append(localSubRegion, LocalSubRegion{
			LocalSubRegion:			data.LocalSubRegion,
			LocalRegion:			data.LocalRegion,
			Country:				data.Country,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &localSubRegion, nil
}

func ConvertToText(rows *sql.Rows) (*[]Text, error) {
	defer rows.Close()
	text := make([]Text, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Text{}

		err := rows.Scan(
			&pm.LocalSubRegion,
			&pm.LocalRegion,
			&pm.Country,
			&pm.Language,
			&pm.LocalSubRegionName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &text, err
		}

		data := pm
		text = append(text, Text{
			LocalSubRegion:     	data.LocalSubRegion,
			LocalRegion:	     	data.LocalRegion,
			Country:				data.Country,
			Language:          		data.Language,
			LocalSubRegionName:		data.LocalSubRegionName,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &text, nil
}
