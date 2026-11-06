package tracker

import "strings"

type trackerFields struct {
	Protocol         string
	DeviceType       string
	Serial           string
	DeviceIdentifier string
}

func extractFields(payload []byte) trackerFields {
	raw := strings.TrimRight(string(payload), "\x00\r\n ")

	if strings.HasPrefix(raw, ">") {
		return extractVirlocFields(payload)
	}

	firstToken := strings.SplitN(raw, ";", 2)[0]

	switch firstToken {
	case "STT", "ALT":
		return extractSuntechFields(payload)
	default:
		return extractRSTFields(payload)
	}
}

func extractRSTFields(payload []byte) trackerFields {
	raw := strings.TrimRight(string(payload), "\x00\r\n ")
	tokens := strings.Split(raw, ";")

	result := trackerFields{Protocol: "UNKNOWN"}

	if len(tokens) > 0 && tokens[0] == "RST" {
		result.Protocol = "RST"
		result.DeviceType = "RST"
	}

	if len(tokens) > 4 && tokens[4] != "" {
		result.Serial = tokens[4]
		result.DeviceIdentifier = tokens[4]
	}

	return result
}

var suntechMessageTypes = map[string]bool{
	"STT": true,
	"ALT": true,
}

func extractSuntechFields(payload []byte) trackerFields {
	raw := strings.TrimRight(string(payload), "\x00\r\n ")
	tokens := strings.Split(raw, ";")

	result := trackerFields{Protocol: "UNKNOWN"}

	if len(tokens) < 2 {
		return result
	}

	msgType := tokens[0]
	if !suntechMessageTypes[msgType] {
		return result
	}

	result.Protocol = "SUNTECH_" + msgType
	result.DeviceType = "SUNTECH"

	if tokens[1] != "" {
		result.Serial = tokens[1]
		result.DeviceIdentifier = tokens[1]
	}

	return result
}

func extractVirlocFields(payload []byte) trackerFields {
	raw := strings.TrimRight(string(payload), "\x00\r\n ")
	raw = strings.TrimPrefix(raw, ">")
	raw = strings.TrimSuffix(raw, "<")

	tokens := strings.Split(raw, ";")

	result := trackerFields{Protocol: "UNKNOWN"}

	if len(tokens[0]) < 3 {
		return result
	}

	result.Protocol = "VIRLOC_" + tokens[0][:3]
	result.DeviceType = "VIRLOC"

	for _, token := range tokens[1:] {
		if id, ok := strings.CutPrefix(token, "ID="); ok {
			result.Serial = id
			result.DeviceIdentifier = id
			break
		}
	}

	return result
}
