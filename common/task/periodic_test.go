package task_test

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/common/task"
)

func TestPeriodicTaskStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var value atomic.Int64
		task := &Periodic{
			Interval: time.Second * 2,
			Execute: func() error {
				value.Add(1)
				return nil
			},
		}
		common.Must(task.Start())
		time.Sleep(time.Second * 5)
		synctest.Wait()
		common.Must(task.Close())
		if got := value.Load(); got != 3 {
			t.Fatalf("configured cadence: got %d executions, want 3", got)
		}
		time.Sleep(time.Second * 4)
		synctest.Wait()
		if got := value.Load(); got != 3 {
			t.Fatalf("closed periodic task executed again: %d", got)
		}
		common.Must(task.Start())
		time.Sleep(time.Second * 3)
		synctest.Wait()
		common.Must(task.Close())
		if got := value.Load(); got != 5 {
			t.Fatalf("restart did not restore configured cadence: %d", got)
		}
	})
}
