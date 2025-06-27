use log::info;
use rand::Rng;
use chrono::{Datelike, Local};
use std::fs::File;
use std::io::{BufWriter, Write};
use split_merge_hub_demo::mt_log::mt_log_record::MTLogRecord;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn random_date<R: Rng>(rng: &mut R) -> String {
    let year = rng.gen_range(2000..=2025);
    let month = rng.gen_range(1..=12);
    let day = rng.gen_range(1..=28); // safe for all months
    format!("{:04}{:02}{:02}", year, month, day)
}

fn random_mt_log_record<R: Rng>(rng: &mut R) -> MTLogRecord {
    MTLogRecord {
        milog_rec_sys_date: random_date(rng).parse().unwrap(),
        milog_rec_sys_time: rng.gen_range(0..=235959),
        milog_rec_taskno: rng.gen_range(1000000..=9999999),
        milog_channel_code: "ATM".to_string(),
        milog_rec_rectype: "1".to_string(),
        milog_ts_ext_tran_code: "TRANCODE".to_string(),
        milog_tran_type: "T".to_string(),
        milog_record_status: "S".to_string(),
        milog_atm_cardnumber: format!("{:016}", rng.gen_range(0..=9999999999999999u64)),
        milog_terminal_id: format!("TERM{:010}", rng.gen_range(0..=9999999999u64)),
        milog_terminal_recno: format!("{:06}", rng.gen_range(0..=999999)),
        milog_ts_teller_id: format!("TELL{:04}", rng.gen_range(0..=9999)),
        milog_ts_tran_serno: rng.gen_range(0..=999999),
        milog_ts_proc_date: random_date(rng),
        milog_eib_tranid: "EIBT".to_string(),
        milog_eib_termid: "EIBT".to_string(),
        milog_cics_applid: "CICS".to_string(),
        milog_next_day_flag: "N".to_string(),
        filler_r1: " ".to_string(),
        mit_isc_cics_tran_code: "CICS".to_string(),
        mit_isc_func_code: "FUNCODE".to_string(),
        mit_isc_front_end_login_id: "LOGINID".to_string(),
        mit_isc_front_end_tran_serno: rng.gen_range(0..=999999),
        mit_isc_reversal_flag: "N".to_string(),
        mit_isc_tran_time: format!("{:06}", rng.gen_range(0..=235959)),
        mit_isc_tran_posting_date: random_date(rng),
        mit_isc_tran_branch_code: format!("{:04}", rng.gen_range(0..=9999)),
        mit_isc_channel_code: "CHN1".to_string(),
        mit_isc_front_end_term_id: "TERMFRONTEND".to_string(),
        mit_isc_front_end_term_recno: format!("{:06}", rng.gen_range(0..=999999)),
        mit_isc_repeat_ind: "N".to_string(),
        mit_mq_channel: "MQCH".to_string(),
        mit_mq_trans_id: "MQTI".to_string(),
        mit_mq_trans_desc: "DESC".to_string(),
        mit_mq_rquid: "RQUID".to_string(),
        mit_acct1_acctnum: format!("{:020}", rng.gen_range(0..=99999999999999999999u128)),
        mit_acct2_acctnum: format!("{:020}", rng.gen_range(0..=99999999999999999999u128)),
        mit_acct3_acctnum: format!("{:010}", rng.gen_range(0..=9999999999u64)),
        mit_acct3_filler: " ".to_string(),
        mit_bank_cd: format!("{:02}", rng.gen_range(0..=99)),
        mit_drcr_ind: "D".to_string(),
        mit_financial_type: "FTYP".to_string(),
        mit_cheque_number: rng.gen_range(0..=9999999999),
        mit_cheque_clrg_type: "CL".to_string(),
        mit_dr_tran_amount: rng.gen_range(0..=999999999999999),
        mit_dr_tran_ccy: "THB".to_string(),
        mit_dr_user_tran_code: "DRUC".to_string(),
        mit_dr_ats_company_id: "ATSID1".to_string(),
        mit_dr_ats_desc: "DAD".to_string(),
        filler_r2: " ".to_string(),
        mit_cr_tran_amount: rng.gen_range(0..=999999999999999),
        mit_cr_tran_ccy: "THB".to_string(),
        mit_cr_user_tran_code: "CRUC".to_string(),
        mit_cr_ats_company_id: "ATSID2".to_string(),
        mit_cr_ats_desc: "CAD".to_string(),
        filler_r3: " ".to_string(),
        mit_chg_tran_amount: rng.gen_range(0..=999999999999999),
        mit_chg_tran_ccy: "THB".to_string(),
        mit_chg_user_tran_code: "CHUC".to_string(),
        mit_chg_tran_desc: "CHGDESC".to_string(),
        mit_fee_process_ind: "F".to_string(),
        mit_fee_type_01: "F01".to_string(),
        mit_fee_amount_01: rng.gen_range(0..=999999999999999),
        mit_fee_type_02: "F02".to_string(),
        mit_fee_amount_02: rng.gen_range(0..=999999999999999),
        mit_fee_type_03: "F03".to_string(),
        mit_fee_amount_03: rng.gen_range(0..=999999999999999),
        mit_fee_type_04: "F04".to_string(),
        mit_fee_amount_04: rng.gen_range(0..=999999999999999),
        mit_fee_type_05: "F05".to_string(),
        mit_fee_amount_05: rng.gen_range(0..=999999999999999),
        mit_fee_type_06: "F06".to_string(),
        mit_fee_amount_06: rng.gen_range(0..=999999999999999),
        mit_fee_type_07: "F07".to_string(),
        mit_fee_amount_07: rng.gen_range(0..=999999999999999),
        mit_fee_type_08: "F08".to_string(),
        mit_fee_amount_08: rng.gen_range(0..=999999999999999),
        mit_fee_type_09: "F09".to_string(),
        mit_fee_amount_09: rng.gen_range(0..=999999999999999),
        mit_fee_type_10: "F10".to_string(),
        mit_fee_amount_10: rng.gen_range(0..=999999999999999),
        mit_bpay_extra_flag: "B".to_string(),
        mit_bpay_extra_data_1: "BPED1".to_string(),
        mit_bpay_extra_data_2: "BPED2".to_string(),
        mit_bpay_extra_data_3: "BPED3".to_string(),
        mit_bpay_value_date: random_date(rng),
        filler_r4: " ".to_string(),
        mit_stop_release_function: "STOPRELFUNC".to_string(),
        mit_wthd_fx_dep_no: "FXD".to_string(),
        mit_wthd_fx_reason: "FXR".to_string(),
        filler_r5: " ".to_string(),
        mit_stmt_chn_desc_acct1: "DESC1".to_string(),
        mit_stmt_chn_desc_acct2: "DESC2".to_string(),
        mit_bpay_partner_acct: "BPPA".to_string(),
        mit_bpay_reconcile_ref: "BPRR".to_string(),
        mit_bpay_interbr_region: "R".to_string(),
        mit_bpay_biller_postdate: random_date(rng),
        mit_bpay_charge_type: "C".to_string(),
        mit_bpay_biller_code: "BILLERCODE".to_string(),
        mit_fcd_tran_code_1: "FCD1".to_string(),
        mit_fcd_tran_code_2: "FCD2".to_string(),
        mit_fcd_tran_code_3: "FCD3".to_string(),
        mit_fcd_tran_code_4: "FCD4".to_string(),
        mit_fcd_udt_1: "UDT1".to_string(),
        mit_fcd_udt_2: "UDT2".to_string(),
        mit_fcd_udt_3: "UDT3".to_string(),
        mit_fcd_total_ccy: "THB".to_string(),
        mit_bpay_ref3: "BPR3".to_string(),
        mit_bpay_send_bank: "BNK".to_string(),
        filler_r6: " ".to_string(),
        mit_fin_annotation_text: "ANNOTXT".to_string(),
        mit_bpay_mcn_verify_flag: "V".to_string(),
        mit_bpay_mcn_confirm_flag: "C".to_string(),
        mit_fin_accum_debit: "1".to_string(),
        mit_fin_accum_credit: "2".to_string(),
        mit_fin_accum_service_type: "ST1".to_string(),
        mit_fin_original_rquid: "ORQRQUID".to_string(),
        mit_stmt_chn_desc_acct3: "DESC3".to_string(),
        mit_2nd_trans_amt: "2222".to_string(),
        mit_2nd_trans_amt_purposed: "2".to_string(),
        mit_2nd_related_ref_no: "REFNO".to_string(),
        filler_r7: " ".to_string(),
        mit_fcd_cr_udt_1: "CRUDT1".to_string(),
        mit_fcd_cr_udt_2: "CRUDT2".to_string(),
        mit_fcd_cr_udt_3: "CRUDT3".to_string(),
        mit_fcd_fe_udt_1: "FEUDT1".to_string(),
        mit_fcd_fe_udt_2: "FEUDT2".to_string(),
        mit_fcd_fe_udt_3: "FEUDT3".to_string(),
        mit_fe_user_tran_code: "FEUC".to_string(),
        filler_log: " ".to_string(),
    }
}

fn main() {
    pretty_env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <rows_per_file>", args[0]);
        std::process::exit(1);
    }
    let rows_per_file: usize = args[1].parse().expect("Please provide a valid number for rows_per_file");
    let output_dir = "large_files";
    std::fs::create_dir_all(output_dir).expect("Failed to create output directory");
    let file1 = format!("{}/mt_log01", output_dir);
    let file2 = format!("{}/mt_log02", output_dir);
    let start = Instant::now();
    rayon::join(
        || generate_mt_log_file_parallel(&file1, rows_per_file),
        || generate_mt_log_file_parallel(&file2, rows_per_file),
    );
    let elapsed = start.elapsed();
    println!("\n✅ Successfully generated 2 MT log files in '{}' directory", output_dir);
    println!("   - {}", file1);
    println!("   - {}", file2);
    println!("   Total rows per file: {}", rows_per_file);
    println!("   Elapsed: {:.2?}", elapsed);
}

fn generate_mt_log_file_parallel(file_path: &str, rows: usize) {
    let batch_size = 100_000;
    let batches = (rows + batch_size - 1) / batch_size;
    println!("📝 Generating: {} ({} rows, {} batches)", file_path, rows, batches);
    let start = Instant::now();
    let mut file = File::create(file_path).expect("Failed to create file");
    let counter = Arc::new(AtomicUsize::new(0));
    (0..batches).into_par_iter().for_each(|batch_idx| {
        let mut rng = rand::thread_rng();
        let start_row = batch_idx * batch_size;
        let end_row = ((batch_idx + 1) * batch_size).min(rows);
        let mut buf = Vec::with_capacity((end_row - start_row) * 4320);
        for _ in start_row..end_row {
            let record = random_mt_log_record(&mut rng);
            buf.extend_from_slice(record.to_fixed_string().as_bytes());
            buf.push(b'\n');
        }
        // Each batch appends to file (sync)
        std::fs::OpenOptions::new()
            .append(true)
            .open(file_path)
            .expect("Open for append").write_all(&buf)
            .expect("Write error");
        let written = counter.fetch_add(end_row - start_row, Ordering::SeqCst) + (end_row - start_row);
        println!("Batch {} done: {} rows written so far", batch_idx + 1, written);
    });
    let elapsed = start.elapsed();
    println!("{}: done {} rows in {:.2?}", file_path, rows, elapsed);
}