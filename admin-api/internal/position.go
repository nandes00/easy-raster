package internal

import (
	"strconv"
	"strings"
	"time"
)

type DevicePosition struct {
	DeviceID  string    `json:"deviceId"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

// ParsePosition extracts GPS coordinates from a raw tracker payload.
// Supports Suntech (STT/ALT) and RST protocols.
func ParsePosition(deviceID, rawPayload string) (*DevicePosition, bool) {
	raw := strings.TrimRight(rawPayload, "\x00\r\n ")
	tokens := strings.Split(raw, ";")
	if len(tokens) == 0 {
		return nil, false
	}

	switch tokens[0] {
	case "STT", "ALT":
		return parseSuntechPosition(deviceID, tokens)
	case "RST":
		return parseRSTPosition(deviceID, tokens)
	}
	return nil, false
}

// Suntech STT/ALT format (observed):
// [0]MsgType;[1]DeviceID;[2]Model;[3]SwVer;[4]FwVer;[5]MsgSeq;
// [6]Date(YYYYMMDD);[7]Time(HH:MM:SS);[8]CellID;[9]MCC;[10]Signal;
// [11]Altitude;[12]Heading;[13]Lat;[14]Lon;[15]Speed;...
func parseSuntechPosition(deviceID string, tokens []string) (*DevicePosition, bool) {
	if len(tokens) < 15 {
		return nil, false
	}

	lat, err := strconv.ParseFloat(tokens[13], 64)
	if err != nil {
		return nil, false
	}
	lon, err := strconv.ParseFloat(tokens[14], 64)
	if err != nil {
		return nil, false
	}

	speed, _ := strconv.ParseFloat(tokens[15], 64)

	ts := parseDateTime(tokens[6], tokens[7])

	return &DevicePosition{
		DeviceID:  deviceID,
		Latitude:  lat,
		Longitude: lon,
		Speed:     speed,
		Timestamp: ts,
	}, true
}

// RST format:
// [0]RST;[1]DevType;[2]MsgType;[3]Version;[4]Serial;
// [5]Date(DDMMYY);[6]Time(HHMMSS);[7]Validity;
// [8]Lat;[9]LatHem;[10]Lon;[11]LonHem;[12]Speed;[13]Heading;...
func parseRSTPosition(deviceID string, tokens []string) (*DevicePosition, bool) {
	if len(tokens) < 13 {
		return nil, false
	}

	lat, err := strconv.ParseFloat(tokens[8], 64)
	if err != nil {
		return nil, false
	}
	lon, err := strconv.ParseFloat(tokens[10], 64)
	if err != nil {
		return nil, false
	}

	if len(tokens) > 9 && tokens[9] == "S" {
		lat = -lat
	}
	if len(tokens) > 11 && tokens[11] == "W" {
		lon = -lon
	}

	speed, _ := strconv.ParseFloat(tokens[12], 64)

	ts := parseDateTime(tokens[5], tokens[6])

	return &DevicePosition{
		DeviceID:  deviceID,
		Latitude:  lat,
		Longitude: lon,
		Speed:     speed,
		Timestamp: ts,
	}, true
}

func parseDateTime(date, timeStr string) time.Time {
	if len(date) == 8 {
		// Suntech: time may be HH:MM:SS or HHMMSS
		if len(timeStr) == 8 {
			t, err := time.Parse("20060102 15:04:05", date+" "+timeStr)
			if err == nil {
				return t.UTC()
			}
		}
		if len(timeStr) == 6 {
			t, err := time.Parse("20060102150405", date+timeStr)
			if err == nil {
				return t.UTC()
			}
		}
	}
	// RST: DDMMYY HHMMSS
	if len(date) == 6 && len(timeStr) == 6 {
		t, err := time.Parse("020106150405", date+timeStr)
		if err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}
