#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"

#include "benchmarks/tpcc_workload.h"

RC tpcc_wl::init(benchmark::TpccWorkload* tpcc_workload_ptr) {
	workload::init();
	string path = "./benchmarks/";
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif
	cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCC schema initialized" << endl;
	init_table(tpcc_workload_ptr);
	next_tid = 0;
	return RCOK;
}

RC tpcc_wl::init_schema(const char * schema_file) {
	workload::init_schema(schema_file);
	t_warehouse = tables["WAREHOUSE"];
	t_district = tables["DISTRICT"];
	t_customer = tables["CUSTOMER"];
	t_history = tables["HISTORY"];
	t_neworder = tables["NEW-ORDER"];
	t_order = tables["ORDER"];
	t_orderline = tables["ORDER-LINE"];
	t_item = tables["ITEM"];
	t_stock = tables["STOCK"];

	i_item = indexes["ITEM_IDX"];
	i_warehouse = indexes["WAREHOUSE_IDX"];
	i_district = indexes["DISTRICT_IDX"];
	i_customer_id = indexes["CUSTOMER_ID_IDX"];
	i_customer_last = indexes["CUSTOMER_LAST_IDX"];
	i_stock = indexes["STOCK_IDX"];
	return RCOK;
}

RC tpcc_wl::init_table(benchmark::TpccWorkload* tpcc_workload_ptr) {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/
	tpcc_buffer = new drand48_data * [g_num_wh];
	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	this->tpcc_workload_ptr_ = tpcc_workload_ptr;
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
	threadInitWarehouse(this);
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_join(p_thds[i], NULL);

	printf("TPCC Data Initialization Complete!\n");
	return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(this);
	return RCOK;
}

// TODO ITEM table is assumed to be in partition 0
void tpcc_wl::init_tab_item(benchmark::TpccWorkload* tpcc_workload_ptr) {
	size_t row_idx = 0;
	tpcc_workload_ptr->item_table_.Resize(g_max_items);

	for (UInt32 i = 1; i <= g_max_items; i++) {
		row_t * row;
		uint64_t row_id;
		
		std::array<char, 24> i_name_array;
		std::array<char, 50> i_data_array;

		t_item->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(I_ID, i);
		uint64_t i_im_id = URand(1L,10000L, 0);
		row->set_value(I_IM_ID, i_im_id);
		char name[24];
		MakeAlphaString(14, 24, name, 0);
		row->set_value(I_NAME, name);
		uint64_t i_price = URand(1, 100, 0);
		row->set_value(I_PRICE, i_price);
		char data[50];
    	MakeAlphaString(26, 50, data, 0);
		// TODO in TPCC, "original" should start at a random position
		if (RAND(10, 0) == 0) 
			strcpy(data, "original");		
		row->set_value(I_DATA, data);

		memcpy( i_name_array.data(), name, 24 );
		memcpy( i_data_array.data(), data, 50 );
		
		index_insert(i_item, i, row, 0);

		tpcc_workload_ptr->item_table_.template set_value_by_key<"I_ID"_tstr>   (row_idx, i);
		tpcc_workload_ptr->item_table_.template set_value_by_key<"I_IM_ID"_tstr>(row_idx, i_im_id);
		tpcc_workload_ptr->item_table_.template set_value_by_key<"I_NAME"_tstr> (row_idx, i_name_array);
		tpcc_workload_ptr->item_table_.template set_value_by_key<"I_PRICE"_tstr>(row_idx, i_price);
		tpcc_workload_ptr->item_table_.template set_value_by_key<"I_DATA"_tstr> (row_idx, i_data_array);
	}
}

void tpcc_wl::init_tab_wh(benchmark::TpccWorkload* tpcc_workload_ptr, uint32_t wid) {
	assert(wid >= 1 && wid <= g_num_wh);
	row_t * row;
	uint64_t row_id;
	t_warehouse->get_new_row(row, 0, row_id);
	row->set_primary_key(wid);
	row->set_value(W_ID, wid);
	char name[10];
    MakeAlphaString(6, 10, name, wid-1);
	row->set_value(W_NAME, name);
	char street[20];
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_CITY, street);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(W_STATE, state);
	char zip[9];
   	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(W_ZIP, zip);
   	double tax = (double)URand(0L,200L,wid-1)/1000.0;
   	double w_ytd=300000.00;
	row->set_value(W_TAX, tax);
	row->set_value(W_YTD, w_ytd);
	
	index_insert(i_warehouse, wid, row, wh_to_part(wid));

	std::array<char, 10> w_name_array;
	memcpy( w_name_array.data(), name, 10 );

	std::array<char, 20> w_street_1_array;
	memcpy( w_street_1_array.data(), street, 20 );

	std::array<char, 20> w_street_2_array;
	memcpy( w_street_2_array.data(), street, 20 );

	std::array<char, 20> w_city_array;
	memcpy( w_city_array.data(), street, 20 );

	std::array<char, 2> w_state_array;
	memcpy( w_state_array.data(), state, 2 );

	std::array<char, 9> w_zip_array;
	memcpy( w_zip_array.data(), zip, 9 );

	const size_t row_idx = 0;

	tpcc_workload_ptr->ware_house_table_.Resize(1);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_ID"_tstr> (row_idx, wid);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_NAME"_tstr>(row_idx, w_name_array);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_STREET_1"_tstr>(row_idx, w_street_1_array);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_STREET_2"_tstr>(row_idx, w_street_2_array);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_CITY"_tstr>(row_idx, w_city_array);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_STATE"_tstr>(row_idx, w_state_array);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_ZIP"_tstr>(row_idx, w_zip_array);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_TAX"_tstr>(row_idx, tax);
	tpcc_workload_ptr->ware_house_table_.template set_value_by_key<"W_YTD"_tstr>(row_idx, w_ytd);
	return;
}

void tpcc_wl::init_tab_dist(benchmark::TpccWorkload* tpcc_workload_ptr, uint64_t wid) {
	size_t row_idx = 0;
	tpcc_workload_ptr->district_table_.Resize(DIST_PER_WARE);

	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
		uint64_t row_id;
		t_district->get_new_row(row, 0, row_id);
		row->set_primary_key(did);
		row->set_value(D_ID, did);
		row->set_value(D_W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name, wid-1);
		row->set_value(D_NAME, name);
		char street[20];
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(D_STATE, state);
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(D_ZIP, zip);
    	double tax = (double)URand(0L,200L,wid-1)/1000.0;
    	double w_ytd=30000.00;
		row->set_value(D_TAX, tax);
		row->set_value(D_YTD, w_ytd);
		row->set_value(D_NEXT_O_ID, 3001);
		
		index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));

		std::array<char, 10> d_name_array;
		memcpy( d_name_array.data(), name, 10 );

		std::array<char, 20> d_street_1_array;
		memcpy( d_street_1_array.data(), street, 20 );

		std::array<char, 20> d_street_2_array;
		memcpy( d_street_2_array.data(), street, 20 );

		std::array<char, 20> d_city_array;
		memcpy( d_city_array.data(), street, 20 );

		std::array<char, 2> d_state_array;
		memcpy( d_state_array.data(), state, 2 );

		std::array<char, 2> d_zip_array;
		memcpy( d_zip_array.data(), zip, 2 );

		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_ID"_tstr>       (row_idx, did);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_W_ID"_tstr>     (row_idx, wid);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_NAME"_tstr>     (row_idx, d_name_array);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_STREET_1"_tstr> (row_idx, d_street_1_array);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_STREET_2"_tstr> (row_idx, d_street_2_array);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_CITY"_tstr>     (row_idx, d_city_array);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_STATE"_tstr>    (row_idx, d_state_array);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_ZIP"_tstr>      (row_idx, d_zip_array);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_TAX"_tstr>      (row_idx, tax);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_YTD"_tstr>      (row_idx, w_ytd);
		tpcc_workload_ptr->district_table_.template set_value_by_key<"D_NEXT_O_ID"_tstr>(row_idx, 3001);
		row_idx++;
	}
}

void tpcc_wl::init_tab_stock(benchmark::TpccWorkload* tpcc_workload_ptr, uint64_t wid) {
	size_t row_idx = 0;
	tpcc_workload_ptr->stock_table_.Resize(g_max_items);
	
	for (UInt32 sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
		uint64_t row_id;
		uint64_t s_quantity = URand(10, 100, wid-1);
		t_stock->get_new_row(row, 0, row_id);
		row->set_primary_key(sid);
		row->set_value(S_I_ID, sid);
		row->set_value(S_W_ID, wid);
		row->set_value(S_QUANTITY, s_quantity);
		row->set_value(S_REMOTE_CNT, 0);
#if !TPCC_SMALL
		char s_dist[25];
		char row_name[10] = "S_DIST_";
		for (int i = 1; i <= 10; i++) {
			if (i < 10) {
				row_name[7] = '0';
				row_name[8] = i + '0';
			} else {
				row_name[7] = '1';
				row_name[8] = '0';
			}
			row_name[9] = '\0';
			MakeAlphaString(24, 24, s_dist, wid-1);
			row->set_value(row_name, s_dist);
		}
		row->set_value(S_YTD, 0);
		row->set_value(S_ORDER_CNT, 0);
		char s_data[50];
		int len = MakeAlphaString(26, 50, s_data, wid-1);
		if (rand() % 100 < 10) {
			int idx = URand(0, len - 8, wid-1);
			strcpy(&s_data[idx], "original");
		}
		row->set_value(S_DATA, s_data);
#endif
		index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));

		tpcc_workload_ptr->stock_table_.template set_value_by_key<"S_I_ID"_tstr>      (row_idx, sid);
		tpcc_workload_ptr->stock_table_.template set_value_by_key<"S_W_ID"_tstr>      (row_idx, wid);
		tpcc_workload_ptr->stock_table_.template set_value_by_key<"S_QUANTITY"_tstr>  (row_idx, s_quantity);
		tpcc_workload_ptr->stock_table_.template set_value_by_key<"S_REMOTE_CNT"_tstr>(row_idx, 0);
		row_idx++;
	}
}

void tpcc_wl::init_tab_cust(benchmark::TpccWorkload* tpcc_workload_ptr, uint64_t did, uint64_t wid) {
	size_t row_idx = 0;
	tpcc_workload_ptr->customer_table_.Resize(g_cust_per_dist);

	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = 1; cid <= g_cust_per_dist; cid++) {
		std::array<char,  2> c_middle_array;
		std::array<char, LASTNAME_LEN> c_last_array;
		std::array<char,  2> c_state_array;
		std::array<char,  2> c_credit_array;
		
		row_t * row;
		uint64_t row_id;
		t_customer->get_new_row(row, 0, row_id);
		row->set_primary_key(cid);
		row->set_value(C_ID, cid);		
		row->set_value(C_D_ID, did);
		row->set_value(C_W_ID, wid);
		char c_last[LASTNAME_LEN];
		if (cid <= 1000)
			Lastname(cid - 1, c_last);
		else
			Lastname(NURand(255,0,999,wid-1), c_last);
		row->set_value(C_LAST, c_last);
		memcpy( c_last_array.data(), c_last, LASTNAME_LEN );

		char tmp[3] = "OE";
		row->set_value(C_MIDDLE, tmp);
		memcpy( c_middle_array.data(), tmp, 2 );

		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(C_STATE, state);
		memcpy( c_state_array.data(), state, 2 );

#if !TPCC_SMALL
		char c_first[FIRSTNAME_LEN];
		MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
		row->set_value(C_FIRST, c_first);
		char street[20];
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_CITY, street); 
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(C_ZIP, zip);
		char phone[16];
  		MakeNumberString(16, 16, phone, wid-1); /* Zip */
		row->set_value(C_PHONE, phone);
		row->set_value(C_SINCE, 0);
		row->set_value(C_CREDIT_LIM, 50000);
		row->set_value(C_DELIVERY_CNT, 0);
		char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
		row->set_value(C_DATA, c_data);
#endif
		if (RAND(10, wid-1) == 0) {
			char tmp[] = "GC";
			row->set_value(C_CREDIT, tmp);
			memcpy( c_credit_array.data(), tmp, 2 );
		} else {
			char tmp[] = "BC";
			row->set_value(C_CREDIT, tmp);
			memcpy( c_credit_array.data(), tmp, 2 );
		}
		double c_discount =  (double)RAND(5000,wid-1) / 10000;
		row->set_value(C_DISCOUNT, c_discount);
		row->set_value(C_BALANCE, -10.0);
		row->set_value(C_YTD_PAYMENT, 10.0);
		row->set_value(C_PAYMENT_CNT, 1);
		uint64_t key;
		key = custNPKey(c_last, did, wid);
		index_insert(i_customer_last, key, row, wh_to_part(wid));
		key = custKey(cid, did, wid);
		index_insert(i_customer_id, key, row, wh_to_part(wid));

		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_ID"_tstr>         (row_idx, cid);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_D_ID"_tstr>       (row_idx, did);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_W_ID"_tstr>       (row_idx, wid);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_MIDDLE"_tstr>     (row_idx, c_middle_array);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_LAST"_tstr>       (row_idx, c_last_array);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_STATE"_tstr>      (row_idx, c_state_array);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_CREDIT"_tstr>     (row_idx, c_credit_array);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_DISCOUNT"_tstr>   (row_idx, c_discount);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_BALANCE"_tstr>    (row_idx, -10.0);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_YTD_PAYMENT"_tstr>(row_idx, 10.0);
		tpcc_workload_ptr->customer_table_.template set_value_by_key<"C_PAYMENT_CNT"_tstr>(row_idx, 1);
		row_idx++;
	}
}

void tpcc_wl::init_tab_hist(benchmark::TpccWorkload* tpcc_workload_ptr, uint64_t c_id, uint64_t d_id, uint64_t w_id) {
	row_t * row;
	uint64_t row_id;
	t_history->get_new_row(row, 0, row_id);
	row->set_primary_key(0);
	row->set_value(H_C_ID, c_id);
	row->set_value(H_C_D_ID, d_id);
	row->set_value(H_D_ID, d_id);
	row->set_value(H_C_W_ID, w_id);
	row->set_value(H_W_ID, w_id);
	row->set_value(H_DATE, 0);
	row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
	char h_data[24];
	MakeAlphaString(12, 24, h_data, w_id-1);
	row->set_value(H_DATA, h_data);
#endif

	const size_t row_idx = 0;

	tpcc_workload_ptr->history_table_.Resize(1);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_C_ID"_tstr> (row_idx, c_id);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_C_D_ID"_tstr>(row_idx, d_id);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_C_W_ID"_tstr>(row_idx, d_id);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_D_ID"_tstr>(row_idx, w_id);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_W_ID"_tstr>(row_idx, w_id);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_DATE"_tstr>(row_idx, 0);
	tpcc_workload_ptr->history_table_.template set_value_by_key<"H_AMOUNT"_tstr>(row_idx, 10.0);
}

void tpcc_wl::init_tab_order(benchmark::TpccWorkload* tpcc_workload_ptr, uint64_t did, uint64_t wid) {
	size_t row_idx = 0;
	tpcc_workload_ptr->order_table_.Resize(g_cust_per_dist);
	tpcc_workload_ptr->new_order_table_.Resize(g_cust_per_dist > 2100? 
											   g_cust_per_dist - 2100 : 0);
	
	uint64_t perm[g_cust_per_dist]; 
	init_permutation(perm, wid); /* initialize permutation of customer numbers */
	for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
		row_t * row;
		uint64_t row_id;
		t_order->get_new_row(row, 0, row_id);
		row->set_primary_key(oid);
		int64_t o_carrier_id = 1;
		uint64_t o_ol_cnt = 1;
		uint64_t cid = perm[oid - 1]; //get_permutation();
		row->set_value(O_ID, oid);
		row->set_value(O_C_ID, cid);
		row->set_value(O_D_ID, did);
		row->set_value(O_W_ID, wid);
		uint64_t o_entry = 2013;
		row->set_value(O_ENTRY_D, o_entry);
		if (oid < 2101) {
			o_carrier_id = URand(1, 10, wid-1);
			row->set_value(O_CARRIER_ID, o_carrier_id);
		}
		else {
			o_carrier_id = 0;
			row->set_value(O_CARRIER_ID, 0);
		}
		o_ol_cnt = URand(5, 15, wid-1);
		row->set_value(O_OL_CNT, o_ol_cnt);
		row->set_value(O_ALL_LOCAL, 1);
		
		// ORDER-LINE	
#if !TPCC_SMALL
		for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
			t_orderline->get_new_row(row, 0, row_id);
			row->set_value(OL_O_ID, oid);
			row->set_value(OL_D_ID, did);
			row->set_value(OL_W_ID, wid);
			row->set_value(OL_NUMBER, ol);
			row->set_value(OL_I_ID, URand(1, 100000, wid-1));
			row->set_value(OL_SUPPLY_W_ID, wid);
			if (oid < 2101) {
				row->set_value(OL_DELIVERY_D, o_entry);
				row->set_value(OL_AMOUNT, 0);
			} else {
				row->set_value(OL_DELIVERY_D, 0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
			}
			row->set_value(OL_QUANTITY, 5);
			char ol_dist_info[24];
	        MakeAlphaString(24, 24, ol_dist_info, wid-1);
			row->set_value(OL_DIST_INFO, ol_dist_info);
		}
#endif

		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_ID"_tstr>        (row_idx, oid);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_C_ID"_tstr>      (row_idx, cid);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_D_ID"_tstr>      (row_idx, did);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_W_ID"_tstr>      (row_idx, wid);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_ENTRY_D"_tstr>   (row_idx, o_entry);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_CARRIER_ID"_tstr>(row_idx, o_carrier_id);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_OL_CNT"_tstr>    (row_idx, o_ol_cnt);
		tpcc_workload_ptr->order_table_.template set_value_by_key<"O_ALL_LOCAL"_tstr> (row_idx, 1);
		row_idx++;

		// NEW ORDER
		if (oid > 2100) {
			t_neworder->get_new_row(row, 0, row_id);
			row->set_value(NO_O_ID, oid);
			row->set_value(NO_D_ID, did);
			row->set_value(NO_W_ID, wid);

			tpcc_workload_ptr->new_order_table_.template set_value_by_key<"NO_O_ID"_tstr>(row_idx - 2100 - 1, oid);
			tpcc_workload_ptr->new_order_table_.template set_value_by_key<"NO_D_ID"_tstr>(row_idx - 2100 - 1, did);
			tpcc_workload_ptr->new_order_table_.template set_value_by_key<"NO_W_ID"_tstr>(row_idx - 2100 - 1, wid);
		}
	}
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
	uint32_t i;
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) 
		perm_c_id[i] = i+1;

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
	tpcc_wl * wl = (tpcc_wl *) This;
	int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	tpcc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	assert((uint64_t)tid < g_num_wh);
	srand48_r(wid, tpcc_buffer[tid]);
	
	if (tid == 0)
		wl->init_tab_item(wl->tpcc_workload_ptr_);
	wl->init_tab_wh( wl->tpcc_workload_ptr_, wid );
	wl->init_tab_dist( wl->tpcc_workload_ptr_, wid );
	wl->init_tab_stock( wl->tpcc_workload_ptr_, wid );
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		wl->init_tab_cust(wl->tpcc_workload_ptr_, did, wid);
		wl->init_tab_order(wl->tpcc_workload_ptr_, did, wid);
		for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) 
			wl->init_tab_hist(wl->tpcc_workload_ptr_, cid, did, wid);
	}
	return NULL;
}
