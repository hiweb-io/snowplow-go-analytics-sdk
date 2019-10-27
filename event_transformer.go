package snowplow

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type convertFunc = func(string, string) ([][]interface{}, error)

var converters = map[string]convertFunc{
	"convertString": func(key, value string) ([][]interface{}, error) {
		return [][]interface{}{{key, value}}, nil
	},
	"convertInt": func(key, value string) ([][]interface{}, error) {
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return [][]interface{}{{key, i}}, nil
	},
	"convertFloat": func(key, value string) ([][]interface{}, error) {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return [][]interface{}{{key, f}}, nil
	},
	"convertBool": func(key, value string) ([][]interface{}, error) {
		if value == "1" {
			return [][]interface{}{{key, true}}, nil
		}
		return [][]interface{}{{key, false}}, nil
	},
	"convertTimestamp": func(key, value string) ([][]interface{}, error) {
		layout := "2006-01-02 15:04:05.000"
		t, err := time.Parse(layout, value)
		if err != nil {
			return nil, err
		}
		return [][]interface{}{{key, t}}, nil
	},
	"convertUnstruct": func(key, value string) ([][]interface{}, error) {
		return parseUnstruct([]byte(value))
	},
	"convertContexts": func(key, value string) ([][]interface{}, error) {
		return parseContexts([]byte(value))
	},
}

// EnrichedEventFieldTypes Ordered list of names of enriched event fields together with the function required to convert them to JSON.
var EnrichedEventFieldTypes = [][]string{
	{"app_id", "convertString"},
	{"platform", "convertString"},
	{"etl_tstamp", "convertTimestamp"},
	{"collector_tstamp", "convertTimestamp"},
	{"dvce_created_tstamp", "convertTimestamp"},
	{"event", "convertString"},
	{"event_id", "convertString"},
	{"txn_id", "convertInt"},
	{"name_tracker", "convertString"},
	{"v_tracker", "convertString"},
	{"v_collector", "convertString"},
	{"v_etl", "convertString"},
	{"user_id", "convertString"},
	{"user_ipaddress", "convertString"},
	{"user_fingerprint", "convertString"},
	{"domain_userid", "convertString"},
	{"domain_sessionidx", "convertInt"},
	{"network_userid", "convertString"},
	{"geo_country", "convertString"},
	{"geo_region", "convertString"},
	{"geo_city", "convertString"},
	{"geo_zipcode", "convertString"},
	{"geo_latitude", "convertFloat"},
	{"geo_longitude", "convertFloat"},
	{"geo_region_name", "convertString"},
	{"ip_isp", "convertString"},
	{"ip_organization", "convertString"},
	{"ip_domain", "convertString"},
	{"ip_netspeed", "convertString"},
	{"page_url", "convertString"},
	{"page_title", "convertString"},
	{"page_referrer", "convertString"},
	{"page_urlscheme", "convertString"},
	{"page_urlhost", "convertString"},
	{"page_urlport", "convertInt"},
	{"page_urlpath", "convertString"},
	{"page_urlquery", "convertString"},
	{"page_urlfragment", "convertString"},
	{"refr_urlscheme", "convertString"},
	{"refr_urlhost", "convertString"},
	{"refr_urlport", "convertInt"},
	{"refr_urlpath", "convertString"},
	{"refr_urlquery", "convertString"},
	{"refr_urlfragment", "convertString"},
	{"refr_medium", "convertString"},
	{"refr_source", "convertString"},
	{"refr_term", "convertString"},
	{"mkt_medium", "convertString"},
	{"mkt_source", "convertString"},
	{"mkt_term", "convertString"},
	{"mkt_content", "convertString"},
	{"mkt_campaign", "convertString"},
	{"contexts", "convertContexts"},
	{"se_category", "convertString"},
	{"se_action", "convertString"},
	{"se_label", "convertString"},
	{"se_property", "convertString"},
	{"se_value", "convertString"},
	{"unstruct_event", "convertUnstruct"},
	{"tr_orderid", "convertString"},
	{"tr_affiliation", "convertString"},
	{"tr_total", "convertFloat"},
	{"tr_tax", "convertFloat"},
	{"tr_shipping", "convertFloat"},
	{"tr_city", "convertString"},
	{"tr_state", "convertString"},
	{"tr_country", "convertString"},
	{"ti_orderid", "convertString"},
	{"ti_sku", "convertString"},
	{"ti_name", "convertString"},
	{"ti_category", "convertString"},
	{"ti_price", "convertFloat"},
	{"ti_quantity", "convertInt"},
	{"pp_xoffset_min", "convertInt"},
	{"pp_xoffset_max", "convertInt"},
	{"pp_yoffset_min", "convertInt"},
	{"pp_yoffset_max", "convertInt"},
	{"useragent", "convertString"},
	{"br_name", "convertString"},
	{"br_family", "convertString"},
	{"br_version", "convertString"},
	{"br_type", "convertString"},
	{"br_renderengine", "convertString"},
	{"br_lang", "convertString"},
	{"br_features_pdf", "convertBool"},
	{"br_features_flash", "convertBool"},
	{"br_features_java", "convertBool"},
	{"br_features_director", "convertBool"},
	{"br_features_quicktime", "convertBool"},
	{"br_features_realplayer", "convertBool"},
	{"br_features_windowsmedia", "convertBool"},
	{"br_features_gears", "convertBool"},
	{"br_features_silverlight", "convertBool"},
	{"br_cookies", "convertBool"},
	{"br_colordepth", "convertString"},
	{"br_viewwidth", "convertInt"},
	{"br_viewheight", "convertInt"},
	{"os_name", "convertString"},
	{"os_family", "convertString"},
	{"os_manufacturer", "convertString"},
	{"os_timezone", "convertString"},
	{"dvce_type", "convertString"},
	{"dvce_ismobile", "convertBool"},
	{"dvce_screenwidth", "convertInt"},
	{"dvce_screenheight", "convertInt"},
	{"doc_charset", "convertString"},
	{"doc_width", "convertInt"},
	{"doc_height", "convertInt"},
	{"tr_currency", "convertString"},
	{"tr_total_base", "convertFloat"},
	{"tr_tax_base", "convertFloat"},
	{"tr_shipping_base", "convertFloat"},
	{"ti_currency", "convertString"},
	{"ti_price_base", "convertFloat"},
	{"base_currency", "convertString"},
	{"geo_timezone", "convertString"},
	{"mkt_clickid", "convertString"},
	{"mkt_network", "convertString"},
	{"etl_tags", "convertString"},
	{"dvce_sent_tstamp", "convertTimestamp"},
	{"refr_domain_userid", "convertString"},
	{"refr_device_tstamp", "convertTimestamp"},
	{"derived_contexts", "convertContexts"},
	{"domain_sessionid", "convertString"},
	{"derived_tstamp", "convertTimestamp"},
	{"event_vendor", "convertString"},
	{"event_name", "convertString"},
	{"event_format", "convertString"},
	{"event_version", "convertString"},
	{"event_fingerprint", "convertString"},
	{"true_tstamp", "convertTimestamp"},
}
const LatitudeIndex = 22
const LongitudeIndex = 23

// Transform Convert a Snowplow enriched event TSV into a map
func Transform(line string, knownFields [][]string, addGeolocationData bool) (map[string]interface{}, error) {
	return jsonifyGoodEvent(strings.Split(line, "\t"), knownFields, addGeolocationData)
}

// jsonifyGoodEvent Convert a Snowplow enriched event in the form of an array of fields into a map
func jsonifyGoodEvent(event []string, knownFields [][]string, addGeolocationData bool) (map[string]interface{}, error) {
	if len(event) != len(knownFields) {
		return nil, fmt.Errorf("expected %d fields, received %d fields", len(knownFields), len(event))
	}
	out := map[string]interface{}{}
	errs := []string{}
	if  addGeolocationData && event[LatitudeIndex] != "" && event[LongitudeIndex] != "" {
		out["geo_location"] = event[LatitudeIndex] + "," + event[LongitudeIndex]
	}
	for i, t := range knownFields {
		k := t[0]
		if event[i] != "" {
			if ok, err := converters[t[1]](k, event[i]); err != nil {
				errs = append(errs, fmt.Sprintf("unexpected exception parsing field with key %s and value %s: %s", k, event[i], err.Error()))
			} else {
				for _, v := range ok {
					out[v[0].(string)] = v[1]
				}
			}
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, ", "))
	}
	return out, nil
}
