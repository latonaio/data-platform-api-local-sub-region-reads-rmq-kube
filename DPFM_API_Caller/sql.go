package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-local-sub-region-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-local-sub-region-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var localSubRegions *[]dpfm_api_output_formatter.LocalSubRegion
	var text *[]dpfm_api_output_formatter.Text
	for _, fn := range accepter {
		switch fn {
		case "LocalSubRegion":
			func() {
				localSubRegions = c.LocalSubRegion(mtx, input, output, errs, log)
			}()
		case "LocalSubRegions":
			func() {
				localSubRegions = c.LocalSubRegions(mtx, input, output, errs, log)
			}()
		case "Text":
			func() {
				text = c.Text(mtx, input, output, errs, log)
			}()
		case "Texts":
			func() {
				text = c.Texts(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		LocalSubRegion: localSubRegions,
		Text:      text,
	}

	return data
}

func (c *DPFMAPICaller) LocalSubRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.LocalSubRegion {
	where := fmt.Sprintf("WHERE LocalSubRegion = '%s'", input.LocalSubRegion.LocalSubRegion)

	where = fmt.Sprintf("%s\nAND LocalRegion = '%s'", where, input.LocalSubRegion.LocalRegion)

	where = fmt.Sprintf("%s\nAND Country = '%s'", where, input.LocalSubRegion.Country)

	if input.LocalSubRegion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.LocalSubRegion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_sub_region_local_sub_region_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, Country ASC, LocalRegion ASC, LocalSubRegion ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToLocalSubRegion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) LocalSubRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.LocalSubRegion {
	where := fmt.Sprintf("WHERE LocalSubRegion = '%s'", input.LocalSubRegion.LocalSubRegion)

	where = fmt.Sprintf("%s\nAND LocalRegion = '%s'", where, input.LocalSubRegion.LocalRegion)

	where = fmt.Sprintf("%s\nAND Country = '%s'", where, input.LocalSubRegion.Country)

	if input.LocalSubRegion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.LocalSubRegion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_sub_region_local_sub_region_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, Country ASC, LocalRegion ASC, LocalSubRegion ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToLocalSubRegion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Text(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Text {
	var args []interface{}
	localSubRegion := input.LocalSubRegion.LocalSubRegion
	localRegion := input.LocalSubRegion.LocalRegion
	country := input.LocalSubRegion.Country
	text := input.LocalSubRegion.Text

	cnt := 0
	for _, v := range text {
		args = append(args, localSubRegion, localRegion, country, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?,?,?,?),", cnt-1) + "(?,?,?,?)"
	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_sub_region_text_data
		WHERE (LocalSubRegion, LocalRegion, Country, Language) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Texts(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Text {
	var args []interface{}
	text := input.LocalSubRegion.Text

	cnt := 0
	for _, v := range text {
		args = append(args, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?),", cnt-1) + "(?)"
	rows, err := c.db.Query(
		`SELECT * 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_sub_region_text_data
		WHERE Language IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	//
	data, err := dpfm_api_output_formatter.ConvertToText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
