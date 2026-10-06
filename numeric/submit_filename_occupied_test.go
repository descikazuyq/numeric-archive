package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“新提交不得覆盖以非默认文件名恢复的旧记录”这一形态。
//
// 读取功能按记录内容识别作业号：目录中只有作业 1、它却保存在
// job-...0002.json（作业 2 的默认命名）时，归档仍应正常打开；此后提交新
// 请求（拟分配作业号 2）必须被拒绝，而不是让新记录占用同一文件、覆盖作业
// 1 的已保存记录。拒绝返回空作业与非空的 ErrFileNameOccupied，不产生新
// 作业、不进入计算队列、不消耗作业号、不登记请求号，也不移动/删除/重命名/
// 改写占用该名称的旧记录。旧记录改名解除冲突并重开后，此前被拒绝的请求
// 取得原本未消耗的作业号 2，按正常规则计算和归档。

// makeSucceededRecord 构造一条带完整成功归档的记录并写入 dir/name，
// 返回其落盘字节与构造用的作业（归档由 newArchive 按真实计算规则生成）。
func makeSucceededRecord(t *testing.T, dir, name string, j *storedJob, sum, sumSq int64) []byte {
	t.Helper()
	j.status = StatusSucceeded
	j.effectiveValues = append([]int64(nil), j.values...)
	j.archive = newArchive(j, append([]int64(nil), j.values...), sum, sumSq, j.finishedAt)
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, name, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return data
}

// 题述主序列：目录中只有作业 1，它保存在作业 2 的默认命名文件中。归档
// 正常打开；提交新请求被拒绝（空作业 + ErrFileNameOccupied），错误给出
// 拟分配作业号、被占用文件名与原归属作业号；拒绝不留任何痕迹、不改动旧
// 记录；幂等重放/冲突不被文件名错误替代；旧记录改名重开后，被拒绝的请求
// 取得未消耗的作业号 2 并正常计算归档，旧作业仍只在改名后的文件中。
func TestSubmitRejectsWhenDefaultFileNameOccupiedByRestoredJob(t *testing.T) {
	dir := t.TempDir()
	when := time.Date(2026, 5, 1, 2, 3, 4, 0, time.UTC)
	old := &storedJob{
		id: 1, submitter: "a", requestID: "r1", seed: 11,
		values:   []int64{3},
		queuedAt: when, startedAt: when, finishedAt: when,
	}
	// 作业 1 的唯一记录恰好占用了作业 2 的默认文件名：作业号由记录内容
	// 识别，这不是重复作业号，归档必须正常打开。
	occupied := jobFileName(2)
	oldBytes := makeSucceededRecord(t, dir, occupied, old, 3, 9)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("目录本身没有重复作业号，应正常打开：%v", err)
	}
	g1, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Sum != 3 ||
		g1.Archive.SumOfSquares != 9 {
		t.Fatalf("作业 1 的成功归档应正常读取：%+v", g1)
	}

	// 拒绝发生在任何写入之前：装上“任何落盘都失败”的故障钩子，被拒绝的
	// 提交仍必须返回文件名占用错误（而不是触发持久化）。
	s.persistFault = func(*storedJob) error { return errors.New("拒绝路径不得尝试落盘") }
	newReq := SubmitRequest{Submitter: "b", RequestID: "new", Values: []int64{9}, Seed: 2}
	reject := func() {
		t.Helper()
		j, err := s.Submit(newReq)
		if j != nil || !errors.Is(err, ErrFileNameOccupied) {
			t.Fatalf("文件名被另一作业占用时必须拒绝：job=%+v err=%v", j, err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "拟分配的新作业号为 2") ||
			!strings.Contains(msg, occupied) || !strings.Contains(msg, "另一作业 1") {
			t.Fatalf("错误应给出拟分配作业号、被占用文件名与原归属作业号：%q", msg)
		}
	}
	reject()
	// 重复提交同样被拒绝：仍不消耗作业号，也不登记请求号。
	reject()
	s.persistFault = nil

	// 被拒绝的请求在任何视角都不可见，作业号与请求号均未消耗。
	if _, err := s.Get(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("被拒绝请求不应能按作业号查询：%v", err)
	}
	if list, err := s.List("b", time.Time{}, time.Time{}); err != nil || len(list) != 0 {
		t.Fatalf("按提交人列举不应看到被拒绝请求：list=%v err=%v", ids(list), err)
	}
	if list, err := s.List("a", time.Time{}, time.Time{}); err != nil || len(list) != 1 || list[0].ID != 1 {
		t.Fatalf("旧作业仍应正常列举：list=%v err=%v", ids(list), err)
	}
	s.mu.Lock()
	_, claimed := s.idem[idemIdentity{"b", "new"}]
	nextID := s.nextID
	owner := s.occupiedNames[occupied]
	s.mu.Unlock()
	if claimed {
		t.Fatal("被拒绝请求的请求号不应被登记")
	}
	if nextID != 2 {
		t.Fatalf("被拒绝请求不得消耗作业号，nextID=%d want 2", nextID)
	}
	if owner != 1 {
		t.Fatalf("占用映射应保持 %s 属于作业 1，got %d", occupied, owner)
	}
	// 目录不增加任何正式记录或临时文件，旧记录字节不变。
	if n := countJobFiles(t, dir); n != 1 {
		t.Fatalf("拒绝提交不得新增记录文件，文件数=%d", n)
	}
	if n := countTmpFiles(t, dir); n != 0 {
		t.Fatalf("拒绝提交不得残留临时文件，临时文件数=%d", n)
	}
	cur, err := os.ReadFile(filepath.Join(dir, occupied))
	if err != nil {
		t.Fatal(err)
	}
	if string(cur) != string(oldBytes) {
		t.Fatal("占用名称的旧记录不得被拒绝的提交改写")
	}
	// 旧作业的参数、状态与已有结果继续按原功能读取，校验值不变。
	g1b, _ := s.Get(1)
	if g1b.Status != StatusSucceeded || g1b.Archive == nil ||
		g1b.Archive.Sum != 3 || g1b.Archive.SumOfSquares != 9 ||
		g1b.Archive.Checksum != g1.Archive.Checksum {
		t.Fatalf("旧作业的状态与归档不得受影响：before=%+v after=%+v", g1, g1b)
	}

	// 名称冲突只影响需要创建记录的新请求：命中已接受作业的幂等重放与冲突
	// 不写文件，必须照旧返回，不能被文件名占用错误替代。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{3}, Seed: 11,
	})
	if err != nil || replay == nil || replay.ID != 1 || replay.Status != StatusSucceeded {
		t.Fatalf("同内容幂等重放应返回原作业当前详情：job=%+v err=%v", replay, err)
	}
	conflict, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{4}, Seed: 11,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, ErrFileNameOccupied) ||
		conflict == nil || conflict.ID != 1 {
		t.Fatalf("内容不同应返回原有幂等冲突与原作业，而非文件名占用错误：job=%+v err=%v",
			conflict, err)
	}
	// 空请求号按新请求处理：仍受文件名占用检查拒绝。
	if j, err := s.Submit(SubmitRequest{Submitter: "b", Values: []int64{1}}); j != nil || !errors.Is(err, ErrFileNameOccupied) {
		t.Fatalf("空请求号的新请求同样应被文件名占用拒绝：job=%+v err=%v", j, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 拒绝路径自始至终没改过旧记录。
	cur, err = os.ReadFile(filepath.Join(dir, occupied))
	if err != nil {
		t.Fatal(err)
	}
	if string(cur) != string(oldBytes) {
		t.Fatal("关闭后旧记录仍应保持原样")
	}

	// 解除冲突：把占用名称的旧记录改成另一个合法且不冲突的文件名，再打开。
	relocated := suffixedRecordName(1)
	if err := os.Rename(filepath.Join(dir, occupied), filepath.Join(dir, relocated)); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("改名解除冲突后应正常打开：%v", err)
	}
	defer func() { _ = s2.Close() }()

	// 此前被拒绝的请求作为新请求接受，取得原本未消耗的作业号 2。
	accepted, err := s2.Submit(newReq)
	if err != nil {
		t.Fatalf("冲突解除后被拒绝的请求应能接受：%v", err)
	}
	if accepted == nil || accepted.ID != 2 {
		t.Fatalf("新请求应取得未消耗的作业号 2：%+v", accepted)
	}
	done := waitStatus(t, s2, 2, StatusSucceeded)
	if done.Archive == nil || done.Seed != 2 || done.Archive.Sum != 9 ||
		done.Archive.SumOfSquares != 81 || len(done.Archive.EffectiveValues) != 1 ||
		done.Archive.EffectiveValues[0] != 9 {
		t.Fatalf("新作业应按正常规则计算归档：%+v", done)
	}

	// 新作业使用默认命名；旧作业仍只在改名后的文件中，没有被复制或覆盖。
	newData, err := os.ReadFile(filepath.Join(dir, jobFileName(2)))
	if err != nil {
		t.Fatalf("新作业应写入默认命名文件：%v", err)
	}
	if !strings.Contains(string(newData), `"id": 2`) ||
		!strings.Contains(string(newData), `"status": "succeeded"`) {
		t.Fatalf("默认命名文件应包含新作业 2 的成功记录：\n%s", newData)
	}
	oldData, err := os.ReadFile(filepath.Join(dir, relocated))
	if err != nil {
		t.Fatal(err)
	}
	if string(oldData) != string(oldBytes) {
		t.Fatal("旧作业改名后的文件不得因新作业提交而改写")
	}
	assertNoDefaultRecord(t, dir, 1)
	if n := countJobFiles(t, dir); n != 2 {
		t.Fatalf("解除冲突后目录应有新旧两份记录，文件数=%d", n)
	}
	rg1, err := s2.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if rg1.Status != StatusSucceeded || rg1.Archive == nil || rg1.Archive.Sum != 3 {
		t.Fatalf("旧作业应继续从改名后的文件读取：%+v", rg1)
	}
	// 两条记录各自的幂等号与列举都正常。
	if list, _ := s2.List("a", time.Time{}, time.Time{}); len(list) != 1 || list[0].ID != 1 {
		t.Fatalf("提交人 a 应只看到作业 1：%v", ids(list))
	}
	if list, _ := s2.List("b", time.Time{}, time.Time{}); len(list) != 1 || list[0].ID != 2 {
		t.Fatalf("提交人 b 应只看到作业 2：%v", ids(list))
	}
	if rp, err := s2.Submit(newReq); err != nil || rp == nil || rp.ID != 2 {
		t.Fatalf("接受后同内容重放应返回作业 2：job=%+v err=%v", rp, err)
	}
}

// 已失败或已取消的旧记录同样占用其文件名：终态记录不是可覆盖的空位置。
func TestSubmitRejectsWhenOccupyingRecordFailedOrCanceled(t *testing.T) {
	for _, st := range []Status{StatusFailed, StatusCanceled} {
		t.Run(string(st), func(t *testing.T) {
			dir := t.TempDir()
			when := time.Date(2026, 6, 1, 2, 3, 4, 0, time.UTC)
			j := &storedJob{
				id: 1, submitter: "a", requestID: "r1",
				values:   []int64{3},
				queuedAt: when, finishedAt: when,
				status:        st,
				failureReason: "既有的终态说明",
			}
			if st == StatusCanceled {
				j.failureReason = ""
			}
			data, err := encodeRecord(j)
			if err != nil {
				t.Fatal(err)
			}
			occupied := jobFileName(2)
			if err := os.WriteFile(filepath.Join(dir, occupied), data, 0o600); err != nil {
				t.Fatal(err)
			}

			s, err := Open(dir)
			if err != nil {
				t.Fatalf("单份非默认命名记录应正常打开：%v", err)
			}
			defer func() { _ = s.Close() }()

			nj, err := s.Submit(SubmitRequest{Submitter: "b", RequestID: "x", Values: []int64{1}})
			if nj != nil || !errors.Is(err, ErrFileNameOccupied) {
				t.Fatalf("%s 终态记录仍占用文件名，必须拒绝：job=%+v err=%v", st, nj, err)
			}
			if !strings.Contains(err.Error(), "另一作业 1") ||
				!strings.Contains(err.Error(), "拟分配的新作业号为 2") {
				t.Fatalf("错误应指出原归属作业 1 与拟分配作业号 2：%q", err)
			}
			// 旧记录原样保留：状态与原因未被改写。
			out, err := os.ReadFile(filepath.Join(dir, occupied))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), `"status": "`+string(st)+`"`) {
				t.Fatalf("旧记录状态必须保持 %s：\n%s", st, out)
			}
			g, _ := s.Get(1)
			if g.Status != st || g.FailureReason != j.failureReason {
				t.Fatalf("旧作业状态与原因不得改变：%+v", g)
			}
			if _, err := s.Get(2); !errors.Is(err, ErrNotFound) {
				t.Fatalf("拒绝不得产生作业 2：%v", err)
			}
			if n := countJobFiles(t, dir); n != 1 {
				t.Fatalf("目录应仍只有旧记录一份文件，文件数=%d", n)
			}
		})
	}
}

// 文件名中的数字不能替代记录中的真实作业号：作业 1 保存在 job-...0005.json
// 中时，拟分配作业号仍是 2 而默认文件 job-...0002.json 空闲，新请求必须
// 正常接受；旧记录继续留在 job-...0005.json 中不被移动或改写。
func TestSubmitAcceptsWhenDigitsInOtherFileNameDoNotMatchNextID(t *testing.T) {
	dir := t.TempDir()
	when := time.Date(2026, 7, 1, 2, 3, 4, 0, time.UTC)
	old := &storedJob{
		id: 1, submitter: "a", requestID: "r1", seed: 4,
		values:   []int64{2},
		queuedAt: when, startedAt: when, finishedAt: when,
	}
	misnamed := jobFileName(5) // 文件名写着 5，记录内容的真实作业号是 1
	oldBytes := makeSucceededRecord(t, dir, misnamed, old, 2, 4)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("非默认命名的单份记录应正常打开：%v", err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.Get(5); !errors.Is(err, ErrNotFound) {
		t.Fatalf("作业号按记录内容识别，文件名中的 5 不是作业 5：%v", err)
	}
	g1, err := s.Get(1)
	if err != nil || g1.Status != StatusSucceeded {
		t.Fatalf("作业 1 应正常读取：g=%+v err=%v", g1, err)
	}

	nj, err := s.Submit(SubmitRequest{Submitter: "b", RequestID: "n", Values: []int64{7}})
	if err != nil {
		t.Fatalf("默认文件空闲时新请求应正常接受，文件名数字不参与作业号识别：%v", err)
	}
	if nj == nil || nj.ID != 2 {
		t.Fatalf("新作业应取得按内容推导出的作业号 2：%+v", nj)
	}
	done := waitStatus(t, s, 2, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 7 {
		t.Fatalf("新作业应正常计算归档：%+v", done)
	}
	// 新作业写默认命名，旧记录仍在名称含 5 的文件中且字节不变。
	if _, err := os.Stat(filepath.Join(dir, jobFileName(2))); err != nil {
		t.Fatalf("新作业应写入默认命名文件：%v", err)
	}
	cur, err := os.ReadFile(filepath.Join(dir, misnamed))
	if err != nil {
		t.Fatal(err)
	}
	if string(cur) != string(oldBytes) {
		t.Fatal("文件名含其他数字的旧记录不得被移动或改写")
	}
	assertNoDefaultRecord(t, dir, 1)
	if n := countJobFiles(t, dir); n != 2 {
		t.Fatalf("目录应包含两份各自合法的记录，文件数=%d", n)
	}
}

// 拒绝发生在接受之前：被拒绝的请求不进入计算队列——作业 2 不会开始运行，
// 也不会留下 running 状态的记录；目录唯一文件始终是旧记录。
func TestRejectedSubmitDoesNotEnterComputeQueue(t *testing.T) {
	dir := t.TempDir()
	when := time.Date(2026, 8, 1, 2, 3, 4, 0, time.UTC)
	old := &storedJob{
		id: 1, submitter: "a",
		values:   []int64{3},
		queuedAt: when, startedAt: when, finishedAt: when,
	}
	makeSucceededRecord(t, dir, jobFileName(2), old, 3, 9)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	computed := make(chan uint64, 4)
	s.compute = func(id uint64, in []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		select {
		case computed <- id:
		default:
		}
		return computeResult(in, seed, canceled)
	}

	if j, err := s.Submit(SubmitRequest{Submitter: "b", RequestID: "q", Values: []int64{1}}); j != nil || !errors.Is(err, ErrFileNameOccupied) {
		t.Fatalf("应拒绝占用文件名的新请求：job=%+v err=%v", j, err)
	}
	// 等待一个调度节拍，确认 worker 从未把作业 2 当作可运行作业启动。
	time.Sleep(20 * time.Millisecond)
	select {
	case id := <-computed:
		t.Fatalf("被拒绝的请求不得进入计算队列，作业 %d 却开始计算", id)
	default:
	}
	if _, err := s.Get(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("作业 2 不应存在：%v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), "job-") && strings.HasSuffix(e.Name(), ".json") &&
			e.Name() != jobFileName(2) {
			t.Fatalf("拒绝不得产生任何新记录文件，发现 %s", e.Name())
		}
	}
}
