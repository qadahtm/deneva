//
// Created by Thamir Qadah on 7/22/17.
//

#include "txn.h"
#include "row.h"
#include "row_silo.h"

#if CC_ALG == SILO

RC
TxnManager::validate_silo()
{
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	uint64_t row_cnt = txn->row_cnt;
	uint64_t wr_cnt = txn->write_cnt;
//	DEBUG_Q("_validation_no_wait = %d; _pre_abort = %d\n", _validation_no_wait, _pre_abort);
//	DEBUG_WL("Test : wr_cnt=%ld, row_cnt = %ld\n", wr_cnt, row_cnt);

	uint64_t write_set[wr_cnt];
	memset(write_set, 0, wr_cnt);
	uint64_t cur_wr_idx = 0;
#if ISOLATION_LEVEL != REPEATABLE_READ
	uint64_t read_set[row_cnt - wr_cnt];
	memset(read_set, 0, row_cnt - wr_cnt);
	uint64_t cur_rd_idx = 0;
#endif
	for (uint64_t rid = 0; rid < row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
#if ISOLATION_LEVEL != REPEATABLE_READ
		else
			read_set[cur_rd_idx ++] = rid;
#endif
	}

	// wr_cnt is unsigned?
	if (wr_cnt > 0){
		// bubble sort the write_set, in primary key order
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
//				DEBUG("Test : wr_cnt=%ld, row_cnt = %ld, write_set[j=%ld] = %ld\n",wr_cnt, row_cnt, j, write_set[j]);
				if (txn->accesses[ write_set[j] ]->orig_row->get_primary_key() >
					txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
				{
					uint64_t tmp = write_set[j];
					write_set[j] = write_set[j+1];
					write_set[j+1] = tmp;
				}
			}
		}
	}


	uint64_t num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;
	if (_pre_abort) {
		for (uint64_t i = 0; i < wr_cnt; i++) {
			row_t * row = txn->accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#if ISOLATION_LEVEL != REPEATABLE_READ
		for (uint64_t i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = txn->accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != txn->accesses[read_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#endif
	}

	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (uint64_t i = 0; i < wr_cnt; i++) {
				row_t * row = txn->accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock())
					break;
#if ATOMIC_WORD
				row->manager->assert_lock();
#endif
				num_locks ++;
				if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == wr_cnt)
				done = true;
			else {
				for (uint64_t i = 0; i < num_locks; i++)
					txn->accesses[ write_set[i] ]->orig_row->manager->release();
				if (_pre_abort) {
					num_locks = 0;
					for (uint64_t i = 0; i < wr_cnt; i++) {
						row_t * row = txn->accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (uint64_t i = 0; i < row_cnt - wr_cnt; i ++) {
						Access * access = txn->accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != txn->accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
				usleep(1);
			}
		}
	} else {
		for (uint64_t i = 0; i < wr_cnt; i++) {
			row_t * row = txn->accesses[ write_set[i] ]->orig_row;
			row->manager->lock();
			num_locks++;
			if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
	}

	// validate rows in the read set
#if ISOLATION_LEVEL != REPEATABLE_READ
	// for repeatable_read, no need to validate the read set.
	for (uint64_t i = 0; i < row_cnt - wr_cnt; i ++) {
		Access * access = txn->accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, false);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
#endif
	// validate rows in the write set
	for (uint64_t i = 0; i < wr_cnt; i++) {
		Access * access = txn->accesses[ write_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, true);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else
		_cur_tid ++;
final:

	if (rc == Abort) {
//		DEBUG_Q("SILO aborting .. \n");
		for (uint64_t i = 0; i < num_locks; i++)
			txn->accesses[ write_set[i] ]->orig_row->manager->release();
//		cleanup(rc);
		start_abort();
	} else {
//		DEBUG_Q("SILO committing .. \n");
		for (uint64_t i = 0; i < wr_cnt; i++) {
			Access * access = txn->accesses[ write_set[i] ];
			access->orig_row->manager->write(
				access->data, _cur_tid );
			txn->accesses[ write_set[i] ]->orig_row->manager->release();
		}
//		cleanup(rc);
		commit();
	}
	return rc;
}
#endif
