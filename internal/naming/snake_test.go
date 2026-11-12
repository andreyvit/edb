package naming

import "testing"

func TestSnake(t *testing.T) {
	for input, want := range map[string]string{
		"": "", "Name": "name", "FirstName": "first_name", "already_snake": "already_snake",
		"ID": "id", "IDs": "ids", "AppID": "app_id", "AppIDs": "app_ids",
		"URLs": "urls", "CustomerUUIDs": "customer_uuids", "ABCs": "abcs",
		"IDsByURL": "ids_by_url", "URLsEnabled": "urls_enabled", "IDs2": "ids2",
		"HTTPServer": "http_server", "HTTPStatus": "http_status", "HTTPS": "https",
		"HTTPSession": "http_session", "HTTPSURL": "httpsurl", "JSONData": "json_data",
		"IDs_Count": "ids_count", "Customer_IDs": "customer_ids", "Version2ID": "version2_id",
		"ÉtatIDs": "état_ids", "ÉTATStatus": "état_status",
	} {
		t.Run(input, func(t *testing.T) {
			if got := Snake(input); got != want {
				t.Fatalf("Snake(%q) = %q, want %q", input, got, want)
			}
		})
	}
}
