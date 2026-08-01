package gateway

import "testing"

func TestParseBackendConfigs(t *testing.T) {
	configs, err := parseBackendConfigs("1=127.0.0.1:9121, 2=dns:///node-2:9122")
	if err != nil {
		t.Fatalf("parseBackendConfigs() error = %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("config count = %d, want 2", len(configs))
	}
	if configs[0].id != 1 || configs[0].address != "127.0.0.1:9121" {
		t.Fatalf("first config = %#v", configs[0])
	}
	if configs[1].id != 2 || configs[1].address != "dns:///node-2:9122" {
		t.Fatalf("second config = %#v", configs[1])
	}
}

func TestParseBackendConfigsRejectsInvalidValues(t *testing.T) {
	tests := []string{
		"",
		"backend",
		"0=127.0.0.1:9121",
		"1=127.0.0.1:9121,1=127.0.0.1:9122",
		"1=127.0.0.1:9121,2=127.0.0.1:9121",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			if _, err := parseBackendConfigs(value); err == nil {
				t.Fatalf("parseBackendConfigs(%q) succeeded", value)
			}
		})
	}
}
