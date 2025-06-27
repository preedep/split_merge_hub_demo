use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct MTLogRecord {
    pub milog_rec_sys_date: u64,
    pub milog_rec_sys_time: u64,
    pub milog_rec_taskno: u64,
    pub milog_channel_code: String,
    pub milog_rec_rectype: String,
    pub milog_ts_ext_tran_code: String,
    pub milog_tran_type: String,
    pub milog_record_status: String,
    pub milog_atm_cardnumber: String,
    pub milog_terminal_id: String,
    pub milog_terminal_recno: String,
    pub milog_ts_teller_id: String,
    pub milog_ts_tran_serno: u64,
    pub milog_ts_proc_date: String,
    pub milog_eib_tranid: String,
    pub milog_eib_termid: String,
    pub milog_cics_applid: String,
    pub milog_next_day_flag: String,
    pub filler_r1: String,
    pub mit_isc_cics_tran_code: String,
    pub mit_isc_func_code: String,
    pub mit_isc_front_end_login_id: String,
    pub mit_isc_front_end_tran_serno: u64,
    pub mit_isc_reversal_flag: String,
    pub mit_isc_tran_time: String,
    pub mit_isc_tran_posting_date: String,
    pub mit_isc_tran_branch_code: String,
    pub mit_isc_channel_code: String,
    pub mit_isc_front_end_term_id: String,
    pub mit_isc_front_end_term_recno: String,
    pub mit_isc_repeat_ind: String,
    pub mit_mq_channel: String,
    pub mit_mq_trans_id: String,
    pub mit_mq_trans_desc: String,
    pub mit_mq_rquid: String,
    pub mit_acct1_acctnum: String,
    pub mit_acct2_acctnum: String,
    pub mit_acct3_acctnum: String,
    pub mit_acct3_filler: String,
    pub mit_bank_cd: String,
    pub mit_drcr_ind: String,
    pub mit_financial_type: String,
    pub mit_cheque_number: u64,
    pub mit_cheque_clrg_type: String,
    pub mit_dr_tran_amount: i64,
    pub mit_dr_tran_ccy: String,
    pub mit_dr_user_tran_code: String,
    pub mit_dr_ats_company_id: String,
    pub mit_dr_ats_desc: String,
    pub filler_r2: String,
    pub mit_cr_tran_amount: i64,
    pub mit_cr_tran_ccy: String,
    pub mit_cr_user_tran_code: String,
    pub mit_cr_ats_company_id: String,
    pub mit_cr_ats_desc: String,
    pub filler_r3: String,
    pub mit_chg_tran_amount: i64,
    pub mit_chg_tran_ccy: String,
    pub mit_chg_user_tran_code: String,
    pub mit_chg_tran_desc: String,
    pub mit_fee_process_ind: String,
    pub mit_fee_type_01: String,
    pub mit_fee_amount_01: i64,
    pub mit_fee_type_02: String,
    pub mit_fee_amount_02: i64,
    pub mit_fee_type_03: String,
    pub mit_fee_amount_03: i64,
    pub mit_fee_type_04: String,
    pub mit_fee_amount_04: i64,
    pub mit_fee_type_05: String,
    pub mit_fee_amount_05: i64,
    pub mit_fee_type_06: String,
    pub mit_fee_amount_06: i64,
    pub mit_fee_type_07: String,
    pub mit_fee_amount_07: i64,
    pub mit_fee_type_08: String,
    pub mit_fee_amount_08: i64,
    pub mit_fee_type_09: String,
    pub mit_fee_amount_09: i64,
    pub mit_fee_type_10: String,
    pub mit_fee_amount_10: i64,
    pub mit_bpay_extra_flag: String,
    pub mit_bpay_extra_data_1: String,
    pub mit_bpay_extra_data_2: String,
    pub mit_bpay_extra_data_3: String,
    pub mit_bpay_value_date: String,
    pub filler_r4: String,
    pub mit_stop_release_function: String,
    pub mit_wthd_fx_dep_no: String,
    pub mit_wthd_fx_reason: String,
    pub filler_r5: String,
    pub mit_stmt_chn_desc_acct1: String,
    pub mit_stmt_chn_desc_acct2: String,
    pub mit_bpay_partner_acct: String,
    pub mit_bpay_reconcile_ref: String,
    pub mit_bpay_interbr_region: String,
    pub mit_bpay_biller_postdate: String,
    pub mit_bpay_charge_type: String,
    pub mit_bpay_biller_code: String,
    pub mit_fcd_tran_code_1: String,
    pub mit_fcd_tran_code_2: String,
    pub mit_fcd_tran_code_3: String,
    pub mit_fcd_tran_code_4: String,
    pub mit_fcd_udt_1: String,
    pub mit_fcd_udt_2: String,
    pub mit_fcd_udt_3: String,
    pub mit_fcd_total_ccy: String,
    pub mit_bpay_ref3: String,
    pub mit_bpay_send_bank: String,
    pub filler_r6: String,
    pub mit_fin_annotation_text: String,
    pub mit_bpay_mcn_verify_flag: String,
    pub mit_bpay_mcn_confirm_flag: String,
    pub mit_fin_accum_debit: String,
    pub mit_fin_accum_credit: String,
    pub mit_fin_accum_service_type: String,
    pub mit_fin_original_rquid: String,
    pub mit_stmt_chn_desc_acct3: String,
    pub mit_2nd_trans_amt: String,
    pub mit_2nd_trans_amt_purposed: String,
    pub mit_2nd_related_ref_no: String,
    pub filler_r7: String,
    pub mit_fcd_cr_udt_1: String,
    pub mit_fcd_cr_udt_2: String,
    pub mit_fcd_cr_udt_3: String,
    pub mit_fcd_fe_udt_1: String,
    pub mit_fcd_fe_udt_2: String,
    pub mit_fcd_fe_udt_3: String,
    pub mit_fe_user_tran_code: String,
    pub filler_log: String,
}

const TOTAL_LENGTH: usize = 4310;

impl MTLogRecord {
    pub fn parse_from_fixed(input: &str) -> Result<Self, String> {
        if input.len() < TOTAL_LENGTH {
            return Err(format!("Input too short: expected {} but got {}", TOTAL_LENGTH, input.len()));
        }
        Ok(Self {
            milog_rec_sys_date: input[0..8].trim().parse::<u64>().unwrap_or(0),
            milog_rec_sys_time: input[8..14].trim().parse::<u64>().unwrap_or(0),
            milog_rec_taskno: input[14..21].trim().parse::<u64>().unwrap_or(0),
            milog_channel_code: input[21..25].trim().to_string(),
            milog_rec_rectype: input[25..26].trim().to_string(),
            milog_ts_ext_tran_code: input[26..34].trim().to_string(),
            milog_tran_type: input[34..35].trim().to_string(),
            milog_record_status: input[35..36].trim().to_string(),
            milog_atm_cardnumber: input[36..52].trim().to_string(),
            milog_terminal_id: input[52..68].trim().to_string(),
            milog_terminal_recno: input[68..74].trim().to_string(),
            milog_ts_teller_id: input[74..82].trim().to_string(),
            milog_ts_tran_serno: input[82..88].trim().parse::<u64>().unwrap_or(0),
            milog_ts_proc_date: input[88..96].trim().to_string(),
            milog_eib_tranid: input[96..100].trim().to_string(),
            milog_eib_termid: input[100..104].trim().to_string(),
            milog_cics_applid: input[104..108].trim().to_string(),
            milog_next_day_flag: input[108..109].trim().to_string(),
            filler_r1: input[109..110].trim().to_string(),
            mit_isc_cics_tran_code: input[110..114].trim().to_string(),
            mit_isc_func_code: input[114..122].trim().to_string(),
            mit_isc_front_end_login_id: input[122..130].trim().to_string(),
            mit_isc_front_end_tran_serno: input[130..136].trim().parse::<u64>().unwrap_or(0),
            mit_isc_reversal_flag: input[136..137].trim().to_string(),
            mit_isc_tran_time: input[137..143].trim().to_string(),
            mit_isc_tran_posting_date: input[143..151].trim().to_string(),
            mit_isc_tran_branch_code: input[151..155].trim().to_string(),
            mit_isc_channel_code: input[155..159].trim().to_string(),
            mit_isc_front_end_term_id: input[159..175].trim().to_string(),
            mit_isc_front_end_term_recno: input[175..181].trim().to_string(),
            mit_isc_repeat_ind: input[181..182].trim().to_string(),
            mit_mq_channel: input[182..186].trim().to_string(),
            mit_mq_trans_id: input[186..190].trim().to_string(),
            mit_mq_trans_desc: input[190..210].trim().to_string(),
            mit_mq_rquid: input[210..246].trim().to_string(),
            mit_acct1_acctnum: input[246..266].trim().to_string(),
            mit_acct2_acctnum: input[266..286].trim().to_string(),
            mit_acct3_acctnum: input[286..296].trim().to_string(),
            mit_acct3_filler: input[296..304].trim().to_string(),
            mit_bank_cd: input[304..306].trim().to_string(),
            mit_drcr_ind: input[306..307].trim().to_string(),
            mit_financial_type: input[307..311].trim().to_string(),
            mit_cheque_number: input[311..321].trim().parse::<u64>().unwrap_or(0),
            mit_cheque_clrg_type: input[321..323].trim().to_string(),
            mit_dr_tran_amount: input[323..338].trim().parse::<i64>().unwrap_or(0),
            mit_dr_tran_ccy: input[338..341].trim().to_string(),
            mit_dr_user_tran_code: input[341..345].trim().to_string(),
            mit_dr_ats_company_id: input[345..351].trim().to_string(),
            mit_dr_ats_desc: input[351..354].trim().to_string(),
            filler_r2: input[354..358].trim().to_string(),
            mit_cr_tran_amount: input[358..373].trim().parse::<i64>().unwrap_or(0),
            mit_cr_tran_ccy: input[373..376].trim().to_string(),
            mit_cr_user_tran_code: input[376..380].trim().to_string(),
            mit_cr_ats_company_id: input[380..386].trim().to_string(),
            mit_cr_ats_desc: input[386..389].trim().to_string(),
            filler_r3: input[389..393].trim().to_string(),
            mit_chg_tran_amount: input[393..408].trim().parse::<i64>().unwrap_or(0),
            mit_chg_tran_ccy: input[408..411].trim().to_string(),
            mit_chg_user_tran_code: input[411..415].trim().to_string(),
            mit_chg_tran_desc: input[415..428].trim().to_string(),
            mit_fee_process_ind: input[428..430].trim().to_string(),
            mit_fee_type_01: input[430..434].trim().to_string(),
            mit_fee_amount_01: input[434..449].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_02: input[449..453].trim().to_string(),
            mit_fee_amount_02: input[453..468].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_03: input[468..472].trim().to_string(),
            mit_fee_amount_03: input[472..487].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_04: input[487..491].trim().to_string(),
            mit_fee_amount_04: input[491..506].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_05: input[506..510].trim().to_string(),
            mit_fee_amount_05: input[510..525].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_06: input[525..529].trim().to_string(),
            mit_fee_amount_06: input[529..544].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_07: input[544..548].trim().to_string(),
            mit_fee_amount_07: input[548..563].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_08: input[563..567].trim().to_string(),
            mit_fee_amount_08: input[567..582].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_09: input[582..586].trim().to_string(),
            mit_fee_amount_09: input[586..601].trim().parse::<i64>().unwrap_or(0),
            mit_fee_type_10: input[601..605].trim().to_string(),
            mit_fee_amount_10: input[605..620].trim().parse::<i64>().unwrap_or(0),
            mit_bpay_extra_flag: input[620..621].trim().to_string(),
            mit_bpay_extra_data_1: input[621..641].trim().to_string(),
            mit_bpay_extra_data_2: input[641..661].trim().to_string(),
            mit_bpay_extra_data_3: input[661..681].trim().to_string(),
            mit_bpay_value_date: input[681..689].trim().to_string(),
            filler_r4: input[689..704].trim().to_string(),
            mit_stop_release_function: input[704..740].trim().to_string(),
            mit_wthd_fx_dep_no: input[740..743].trim().to_string(),
            mit_wthd_fx_reason: input[743..745].trim().to_string(),
            filler_r5: input[745..815].trim().to_string(),
            mit_stmt_chn_desc_acct1: input[815..865].trim().to_string(),
            mit_stmt_chn_desc_acct2: input[865..915].trim().to_string(),
            mit_bpay_partner_acct: input[915..935].trim().to_string(),
            mit_bpay_reconcile_ref: input[935..949].trim().to_string(),
            mit_bpay_interbr_region: input[949..950].trim().to_string(),
            mit_bpay_biller_postdate: input[950..956].trim().to_string(),
            mit_bpay_charge_type: input[956..957].trim().to_string(),
            mit_bpay_biller_code: input[957..974].trim().to_string(),
            mit_fcd_tran_code_1: input[974..978].trim().to_string(),
            mit_fcd_tran_code_2: input[978..982].trim().to_string(),
            mit_fcd_tran_code_3: input[982..986].trim().to_string(),
            mit_fcd_tran_code_4: input[986..990].trim().to_string(),
            mit_fcd_udt_1: input[990..1050].trim().to_string(),
            mit_fcd_udt_2: input[1050..1110].trim().to_string(),
            mit_fcd_udt_3: input[1110..1170].trim().to_string(),
            mit_fcd_total_ccy: input[1170..1173].trim().to_string(),
            mit_bpay_ref3: input[1173..1193].trim().to_string(),
            mit_bpay_send_bank: input[1193..1196].trim().to_string(),
            filler_r6: input[1196..1223].trim().to_string(),
            mit_fin_annotation_text: input[1223..1273].trim().to_string(),
            mit_bpay_mcn_verify_flag: input[1273..1274].trim().to_string(),
            mit_bpay_mcn_confirm_flag: input[1274..1275].trim().to_string(),
            mit_fin_accum_debit: input[1275..1276].trim().to_string(),
            mit_fin_accum_credit: input[1276..1277].trim().to_string(),
            mit_fin_accum_service_type: input[1277..1280].trim().to_string(),
            mit_fin_original_rquid: input[1280..1316].trim().to_string(),
            mit_stmt_chn_desc_acct3: input[1316..1366].trim().to_string(),
            mit_2nd_trans_amt: input[1366..1381].trim().to_string(),
            mit_2nd_trans_amt_purposed: input[1381..1382].trim().to_string(),
            mit_2nd_related_ref_no: input[1382..1398].trim().to_string(),
            filler_r7: input[1398..1427].trim().to_string(),
            mit_fcd_cr_udt_1: input[1427..1487].trim().to_string(),
            mit_fcd_cr_udt_2: input[1487..1547].trim().to_string(),
            mit_fcd_cr_udt_3: input[1547..1607].trim().to_string(),
            mit_fcd_fe_udt_1: input[1607..1667].trim().to_string(),
            mit_fcd_fe_udt_2: input[1667..1727].trim().to_string(),
            mit_fcd_fe_udt_3: input[1727..1787].trim().to_string(),
            mit_fe_user_tran_code: input[1787..1791].trim().to_string(),
            filler_log: input[1791..4310].trim().to_string(),
        })
    }

    /// Write MTLogRecord as fixed-length string (4310 chars)
    pub fn to_fixed_string(&self) -> String {
        let mut s = String::with_capacity(TOTAL_LENGTH);
        s.push_str(&format!("{:0>8}", self.milog_rec_sys_date));
        s.push_str(&format!("{:0>6}", self.milog_rec_sys_time));
        s.push_str(&format!("{:0>7}", self.milog_rec_taskno));
        s.push_str(&format!("{:<4}", self.milog_channel_code));
        s.push_str(&format!("{:<1}", self.milog_rec_rectype));
        s.push_str(&format!("{:<8}", self.milog_ts_ext_tran_code));
        s.push_str(&format!("{:<1}", self.milog_tran_type));
        s.push_str(&format!("{:<1}", self.milog_record_status));
        s.push_str(&format!("{:<16}", self.milog_atm_cardnumber));
        s.push_str(&format!("{:<16}", self.milog_terminal_id));
        s.push_str(&format!("{:<6}", self.milog_terminal_recno));
        s.push_str(&format!("{:<8}", self.milog_ts_teller_id));
        s.push_str(&format!("{:0>6}", self.milog_ts_tran_serno));
        s.push_str(&format!("{:<8}", self.milog_ts_proc_date));
        s.push_str(&format!("{:<4}", self.milog_eib_tranid));
        s.push_str(&format!("{:<4}", self.milog_eib_termid));
        s.push_str(&format!("{:<4}", self.milog_cics_applid));
        s.push_str(&format!("{:<1}", self.milog_next_day_flag));
        s.push_str(&format!("{:<1}", self.filler_r1));
        s.push_str(&format!("{:<4}", self.mit_isc_cics_tran_code));
        s.push_str(&format!("{:<8}", self.mit_isc_func_code));
        s.push_str(&format!("{:<8}", self.mit_isc_front_end_login_id));
        s.push_str(&format!("{:0>6}", self.mit_isc_front_end_tran_serno));
        s.push_str(&format!("{:<1}", self.mit_isc_reversal_flag));
        s.push_str(&format!("{:<6}", self.mit_isc_tran_time));
        s.push_str(&format!("{:<8}", self.mit_isc_tran_posting_date));
        s.push_str(&format!("{:<4}", self.mit_isc_tran_branch_code));
        s.push_str(&format!("{:<4}", self.mit_isc_channel_code));
        s.push_str(&format!("{:<16}", self.mit_isc_front_end_term_id));
        s.push_str(&format!("{:<6}", self.mit_isc_front_end_term_recno));
        s.push_str(&format!("{:<1}", self.mit_isc_repeat_ind));
        s.push_str(&format!("{:<4}", self.mit_mq_channel));
        s.push_str(&format!("{:<4}", self.mit_mq_trans_id));
        s.push_str(&format!("{:<20}", self.mit_mq_trans_desc));
        s.push_str(&format!("{:<36}", self.mit_mq_rquid));
        s.push_str(&format!("{:<20}", self.mit_acct1_acctnum));
        s.push_str(&format!("{:<20}", self.mit_acct2_acctnum));
        s.push_str(&format!("{:<10}", self.mit_acct3_acctnum));
        s.push_str(&format!("{:<8}", self.mit_acct3_filler));
        s.push_str(&format!("{:<2}", self.mit_bank_cd));
        s.push_str(&format!("{:<1}", self.mit_drcr_ind));
        s.push_str(&format!("{:<4}", self.mit_financial_type));
        s.push_str(&format!("{:0>10}", self.mit_cheque_number));
        s.push_str(&format!("{:<2}", self.mit_cheque_clrg_type));
        s.push_str(&format!("{:0>15}", self.mit_dr_tran_amount));
        s.push_str(&format!("{:<3}", self.mit_dr_tran_ccy));
        s.push_str(&format!("{:<4}", self.mit_dr_user_tran_code));
        s.push_str(&format!("{:<6}", self.mit_dr_ats_company_id));
        s.push_str(&format!("{:<3}", self.mit_dr_ats_desc));
        s.push_str(&format!("{:<4}", self.filler_r2));
        s.push_str(&format!("{:0>15}", self.mit_cr_tran_amount));
        s.push_str(&format!("{:<3}", self.mit_cr_tran_ccy));
        s.push_str(&format!("{:<4}", self.mit_cr_user_tran_code));
        s.push_str(&format!("{:<6}", self.mit_cr_ats_company_id));
        s.push_str(&format!("{:<3}", self.mit_cr_ats_desc));
        s.push_str(&format!("{:<4}", self.filler_r3));
        s.push_str(&format!("{:0>15}", self.mit_chg_tran_amount));
        s.push_str(&format!("{:<3}", self.mit_chg_tran_ccy));
        s.push_str(&format!("{:<4}", self.mit_chg_user_tran_code));
        s.push_str(&format!("{:<13}", self.mit_chg_tran_desc));
        s.push_str(&format!("{:<2}", self.mit_fee_process_ind));
        s.push_str(&format!("{:<4}", self.mit_fee_type_01));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_01));
        s.push_str(&format!("{:<4}", self.mit_fee_type_02));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_02));
        s.push_str(&format!("{:<4}", self.mit_fee_type_03));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_03));
        s.push_str(&format!("{:<4}", self.mit_fee_type_04));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_04));
        s.push_str(&format!("{:<4}", self.mit_fee_type_05));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_05));
        s.push_str(&format!("{:<4}", self.mit_fee_type_06));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_06));
        s.push_str(&format!("{:<4}", self.mit_fee_type_07));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_07));
        s.push_str(&format!("{:<4}", self.mit_fee_type_08));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_08));
        s.push_str(&format!("{:<4}", self.mit_fee_type_09));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_09));
        s.push_str(&format!("{:<4}", self.mit_fee_type_10));
        s.push_str(&format!("{:0>15}", self.mit_fee_amount_10));
        s.push_str(&format!("{:<1}", self.mit_bpay_extra_flag));
        s.push_str(&format!("{:<20}", self.mit_bpay_extra_data_1));
        s.push_str(&format!("{:<20}", self.mit_bpay_extra_data_2));
        s.push_str(&format!("{:<20}", self.mit_bpay_extra_data_3));
        s.push_str(&format!("{:<8}", self.mit_bpay_value_date));
        s.push_str(&format!("{:<15}", self.filler_r4));
        s.push_str(&format!("{:<36}", self.mit_stop_release_function));
        s.push_str(&format!("{:<3}", self.mit_wthd_fx_dep_no));
        s.push_str(&format!("{:<2}", self.mit_wthd_fx_reason));
        s.push_str(&format!("{:<70}", self.filler_r5));
        s.push_str(&format!("{:<50}", self.mit_stmt_chn_desc_acct1));
        s.push_str(&format!("{:<50}", self.mit_stmt_chn_desc_acct2));
        s.push_str(&format!("{:<20}", self.mit_bpay_partner_acct));
        s.push_str(&format!("{:<14}", self.mit_bpay_reconcile_ref));
        s.push_str(&format!("{:<1}", self.mit_bpay_interbr_region));
        s.push_str(&format!("{:<6}", self.mit_bpay_biller_postdate));
        s.push_str(&format!("{:<1}", self.mit_bpay_charge_type));
        s.push_str(&format!("{:<17}", self.mit_bpay_biller_code));
        s.push_str(&format!("{:<4}", self.mit_fcd_tran_code_1));
        s.push_str(&format!("{:<4}", self.mit_fcd_tran_code_2));
        s.push_str(&format!("{:<4}", self.mit_fcd_tran_code_3));
        s.push_str(&format!("{:<4}", self.mit_fcd_tran_code_4));
        s.push_str(&format!("{:<60}", self.mit_fcd_udt_1));
        s.push_str(&format!("{:<60}", self.mit_fcd_udt_2));
        s.push_str(&format!("{:<60}", self.mit_fcd_udt_3));
        s.push_str(&format!("{:<3}", self.mit_fcd_total_ccy));
        s.push_str(&format!("{:<20}", self.mit_bpay_ref3));
        s.push_str(&format!("{:<3}", self.mit_bpay_send_bank));
        s.push_str(&format!("{:<27}", self.filler_r6));
        s.push_str(&format!("{:<50}", self.mit_fin_annotation_text));
        s.push_str(&format!("{:<1}", self.mit_bpay_mcn_verify_flag));
        s.push_str(&format!("{:<1}", self.mit_bpay_mcn_confirm_flag));
        s.push_str(&format!("{:<1}", self.mit_fin_accum_debit));
        s.push_str(&format!("{:<1}", self.mit_fin_accum_credit));
        s.push_str(&format!("{:<3}", self.mit_fin_accum_service_type));
        s.push_str(&format!("{:<36}", self.mit_fin_original_rquid));
        s.push_str(&format!("{:<50}", self.mit_stmt_chn_desc_acct3));
        s.push_str(&format!("{:<15}", self.mit_2nd_trans_amt));
        s.push_str(&format!("{:<1}", self.mit_2nd_trans_amt_purposed));
        s.push_str(&format!("{:<16}", self.mit_2nd_related_ref_no));
        s.push_str(&format!("{:<29}", self.filler_r7));
        s.push_str(&format!("{:<60}", self.mit_fcd_cr_udt_1));
        s.push_str(&format!("{:<60}", self.mit_fcd_cr_udt_2));
        s.push_str(&format!("{:<60}", self.mit_fcd_cr_udt_3));
        s.push_str(&format!("{:<60}", self.mit_fcd_fe_udt_1));
        s.push_str(&format!("{:<60}", self.mit_fcd_fe_udt_2));
        s.push_str(&format!("{:<60}", self.mit_fcd_fe_udt_3));
        s.push_str(&format!("{:<4}", self.mit_fe_user_tran_code));
        s.push_str(&format!("{:<2519}", self.filler_log));
        s.truncate(TOTAL_LENGTH);
        s
    }
}