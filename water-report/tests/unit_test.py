"""不依赖浏览器/服务的单元测试：CSV 校验、本地状态机、崩溃恢复。"""
import sqlite3

import pytest

from reporter import db
from reporter.csvkit import CsvError, parse_csv


def csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class TestCsv:
    def test_basic_parse(self):
        rows = parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,7.2\nS2,COD,24\n"))
        assert len(rows) == 2
        assert rows[0].sample_no == "S1"
        assert rows[0].result == "7.2"
        assert rows[1].seq == 2

    def test_english_headers_accepted(self):
        rows = parse_csv(csv_bytes("sample_no,project,result\nS1,pH,0\n"))
        assert rows[0].result == "0"

    def test_negative_rejected(self):
        with pytest.raises(CsvError, match="非负"):
            parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,-0.1\n"))

    def test_non_decimal_rejected(self):
        with pytest.raises(CsvError, match="十进制"):
            parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,abc\n"))

    def test_infinity_rejected(self):
        with pytest.raises(CsvError):
            parse_csv(csv_bytes("样本号,项目,结果\nS1,pH,Infinity\n"))

    def test_duplicate_key_rejected(self):
        with pytest.raises(CsvError, match="重复"):
            parse_csv(csv_bytes(
                "样本号,项目,结果\nS1,pH,1\nS1,pH,2\n"))

    def test_missing_header_rejected(self):
        with pytest.raises(CsvError, match="表头"):
            parse_csv(csv_bytes("a,b,c\nS1,pH,1\n"))

    def test_blank_lines_skipped(self):
        rows = parse_csv(csv_bytes(
            "样本号,项目,结果\nS1,pH,1\n\n  \nS2,pH,2\n"))
        assert len(rows) == 2

    def test_gb18030_encoding(self):
        text = "样本号,项目,结果\nS1,pH,1\n".encode("gb18030")
        rows = parse_csv(text)
        assert rows[0].sample_no == "S1"


@pytest.fixture()
def conn(tmp_path):
    path = str(tmp_path / "test.db")
    db.init_db(path)
    c = db.connect(path)
    yield c
    c.close()


class TestStateMachine:
    def _batch_with_row(self, conn, status=db.PENDING):
        bid = db.create_batch(conn, "f.csv")
        rid = db.add_row(conn, bid, 1, "S1", "pH", "7.2")
        if status != db.PENDING:
            db.update_row(conn, rid, status=status)
        return bid, rid

    def test_lock_is_single_slot(self, conn):
        bid, _ = self._batch_with_row(conn)
        wid = db.acquire_lock(conn, bid)
        assert wid
        with pytest.raises(db.LockBusy):
            db.acquire_lock(conn, bid)
        db.release_lock(conn, wid)
        # 释放后可重新获取
        assert db.acquire_lock(conn, bid)

    def test_recover_saving_to_unverified(self, conn):
        bid, rid = self._batch_with_row(conn, status=db.SAVING)
        db.acquire_lock(conn, bid)  # 模拟进程持锁崩溃
        db.recover_stale_runs(conn)
        assert db.current_lock(conn) is None
        row = db.get_row(conn, rid)
        assert row["status"] == db.UNVERIFIED
        assert "待核实" in row["message"]
        batch = db.get_batch(conn, bid)
        assert batch["status"] == db.BATCH_STOPPED

    def test_terminal_states_not_actionable(self, conn):
        for st in (db.SAVED, db.ALREADY_SENT, db.CONFLICT):
            assert st not in db.ACTIONABLE
        for st in (db.PENDING, db.QUERY_FAILED, db.UNVERIFIED):
            assert st in db.ACTIONABLE

    def test_events_append(self, conn):
        _, rid = self._batch_with_row(conn)
        db.add_event(conn, rid, "SAVING", "detail-x")
        db.add_event(conn, rid, "SAVED", "remote_id=9")
        events = db.get_events(conn, rid)
        assert [e["event"] for e in events] == ["SAVING", "SAVED"]
        assert events[1]["detail"] == "remote_id=9"

    def test_row_counts(self, conn):
        bid = db.create_batch(conn, "f.csv")
        db.add_row(conn, bid, 1, "S1", "pH", "1")
        db.add_row(conn, bid, 2, "S2", "pH", "2")
        db.update_row(conn, 1, status=db.SAVED, remote_id="10")
        counts = db.row_counts(conn, bid)
        assert counts[db.SAVED] == 1
        assert counts[db.PENDING] == 1


class TestAtomicBatchInsert:
    def _rows(self, n=3):
        from reporter.csvkit import CsvRow
        return [CsvRow(i + 1, f"S{i}", "pH", f"{i}.5") for i in range(n)]

    def test_batch_and_rows_inserted_in_one_commit(self, conn):
        bid = db.create_batch_with_rows(conn, "f.csv", self._rows())
        assert db.get_batch(conn, bid) is not None
        rows = db.get_rows(conn, bid)
        assert len(rows) == 3
        assert all(r["status"] == db.PENDING for r in rows)

    def test_failure_rolls_back_no_batch_left(self, conn):
        from reporter import db as dbmod

        class FailingConn:
            """在 executemany 上抛锁错误，其余方法委托真实连接。"""
            def __init__(self, real):
                self._real = real

            def execute(self, *a, **k):
                return self._real.execute(*a, **k)

            def executemany(self, *a, **k):
                raise sqlite3.OperationalError("database is locked")

            def commit(self):
                return self._real.commit()

            def rollback(self):
                return self._real.rollback()

        before = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        with pytest.raises(dbmod.LockTimeout):
            db.create_batch_with_rows(FailingConn(conn), "f.csv", self._rows())
        after = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        # 整体回滚：没有留下批次，也没有残留行
        assert after == before
        assert conn.execute("SELECT COUNT(*) FROM rows").fetchone()[0] == 0
        # 回滚后连接仍可正常使用（事务已干净结束）
        bid = db.create_batch(conn, "g.csv")
        assert db.get_batch(conn, bid) is not None
