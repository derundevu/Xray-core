package platform_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/common/platform"
)

func TestNormalizeEnvName(t *testing.T) {
	cases := []struct {
		input  string
		output string
	}{
		{
			input:  "a",
			output: "A",
		},
		{
			input:  "a.a",
			output: "A_A",
		},
		{
			input:  "A.A.B",
			output: "A_A_B",
		},
	}
	for _, test := range cases {
		if v := NormalizeEnvName(test.input); v != test.output {
			t.Error("unexpected output: ", v, " want ", test.output)
		}
	}
}

func TestEnvFlag(t *testing.T) {
	v := EnvFlag{
		Name: "xxxxx.y",
	}.GetValueAsInt(10)
	if v != 10 {
		t.Error("env value: ", v)
	}
}

func TestGetAssetLocation(t *testing.T) {
	// Test the default independently of inherited asset directories.
	for _, name := range []string{AssetLocation, NormalizeEnvName(AssetLocation)} {
		t.Setenv(name, "")
		common.Must(os.Unsetenv(name))
	}

	exec, err := os.Executable()
	common.Must(err)

	loc := GetAssetLocation("t")
	if filepath.Dir(loc) != filepath.Dir(exec) {
		t.Error("asset dir: ", loc, " not in ", exec)
	}

	assetDir := t.TempDir()
	t.Setenv(NormalizeEnvName(AssetLocation), assetDir)
	if v := GetAssetLocation("t"); v != filepath.Join(assetDir, "t") {
		t.Error("asset loc: ", v)
	}

	t.Setenv(AssetLocation, "/xray")
	if runtime.GOOS == "windows" {
		if v := GetAssetLocation("t"); v != "\\xray\\t" {
			t.Error("asset loc: ", v)
		}
	} else {
		if v := GetAssetLocation("t"); v != "/xray/t" {
			t.Error("asset loc: ", v)
		}
	}
}
