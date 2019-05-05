package common

import (
	"fmt"
	"os"
)

func CheckError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error [%s]", err.Error())
		return err
	}
	return nil
}