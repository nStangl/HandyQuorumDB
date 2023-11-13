package client

import (
	"fmt"
	"strings"

	"github.com/nStangl/distributed-kv-store/protocol"
)

func ValidateInput(in []string, n int) error {
	if len(in) != n {
		return fmt.Errorf("invalid format")
	}
	return nil
}

func ValidatePutResponse(resp *protocol.ClientMessage, printPrefix string) (string, error) {
	expected := []protocol.ClientType{
		protocol.PutSuccess,
		protocol.PutError,
		protocol.PutUpdate,
		protocol.ServerNotResponsible,
	}

	return validateClientResponse(resp, expected, printPrefix)

}

func ValidateGetResponse(resp *protocol.ClientMessage, printPrefix string) (string, error) {
	expected := []protocol.ClientType{
		protocol.GetSuccess,
		protocol.GetError,
		protocol.ServerNotResponsible,
	}

	return validateClientResponse(resp, expected, printPrefix)
}

func ValidateDeleteResponse(resp *protocol.ClientMessage, printPrefix string) (string, error) {
	expected := []protocol.ClientType{
		protocol.DeleteSucess,
		protocol.DeleteError,
		protocol.ServerNotResponsible,
	}

	return validateClientResponse(resp, expected, printPrefix)
}

// validateClientResponse takes the reponse and a list of expected reponse message types
// and returns printable message of successfull or error for unexpected repsonse.
// Note that PutError is an expected reponse type for Put requests.
func validateClientResponse(resp *protocol.ClientMessage, expected []protocol.ClientType, printPrefix string) (string, error) {
	if resp == nil {
		return "", fmt.Errorf("received null data")
	}

	for _, e := range expected {
		if resp.Type == e {
			msg := fmt.Sprintf("%s %s %s %s", printPrefix, resp.Type.String(), resp.Key, resp.Value)
			// Remove empty space at end if value is empty
			return strings.TrimSpace(msg), nil
		}
	}

	err := fmt.Errorf("received unexpected reply to %s message: %#v", resp.Type, resp)
	return "", err
}
