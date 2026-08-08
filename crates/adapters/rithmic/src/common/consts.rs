// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 or later.
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic venue constants.

use std::sync::LazyLock;

use nautilus_model::identifiers::Venue;

/// The Rithmic venue identifier string.
pub const RITHMIC_VENUE: &str = "RITHMIC";
/// The Rithmic venue.
pub static RITHMIC_VENUE_ID: LazyLock<Venue> = LazyLock::new(|| Venue::from(RITHMIC_VENUE));

/// Exchange MIC codes for Rithmic-supported exchanges.
pub mod exchanges {
    /// Chicago Mercantile Exchange.
    pub const CME: &str = "CME";
    /// Chicago Board of Trade.
    pub const CBOT: &str = "CBOT";
    /// New York Mercantile Exchange.
    pub const NYMEX: &str = "NYMEX";
    /// Commodity Exchange (gold, silver, copper).
    pub const COMEX: &str = "COMEX";
    /// All hard-coded supported exchanges for bulk loading.
    ///
    /// Used by `RithmicInstrumentProvider::load_all_async()` to iterate
    /// through the supported exchange set only.
    pub const KNOWN_EXCHANGES: &[&str] = &[CME, CBOT, NYMEX, COMEX];
}

/// Common futures product codes.
pub mod products {
    // CME equity futures.
    /// E-mini S&P 500.
    pub const ES: &str = "ES";
    /// Nikkei 225.
    pub const NKD: &str = "NKD";
    /// E-mini NASDAQ 100.
    pub const NQ: &str = "NQ";
    /// E-mini Midcap 400.
    pub const EMD: &str = "EMD";
    /// E-mini Russell 2000.
    pub const RTY: &str = "RTY";

    // CBOT equity futures.
    /// Mini-Dow.
    pub const YM: &str = "YM";

    // CME micro index and crypto futures.
    /// Micro E-mini S&P 500.
    pub const MES: &str = "MES";
    /// Micro E-mini NASDAQ 100.
    pub const MNQ: &str = "MNQ";
    /// Micro E-mini Russell 2000.
    pub const M2K: &str = "M2K";
    /// Micro E-mini Dow Jones.
    ///
    /// Depending on the connected Rithmic system this can route via CBOT or CME.
    pub const MYM: &str = "MYM";
    /// Micro Bitcoin.
    pub const MBT: &str = "MBT";
    /// Micro Ether.
    pub const MET: &str = "MET";

    // CME foreign exchange futures.
    /// Australian Dollar.
    pub const FX_6A: &str = "6A";
    /// British Pound.
    pub const FX_6B: &str = "6B";
    /// Canadian Dollar.
    pub const FX_6C: &str = "6C";
    /// Euro FX.
    pub const FX_6E: &str = "6E";
    /// Japanese Yen.
    pub const FX_6J: &str = "6J";
    /// Swiss Franc.
    pub const FX_6S: &str = "6S";
    /// E-mini Euro FX.
    pub const FX_E7: &str = "E7";
    /// Micro Euro FX.
    pub const FX_M6E: &str = "M6E";
    /// Micro AUD/USD.
    pub const FX_M6A: &str = "M6A";
    /// Mexican Peso.
    pub const FX_6M: &str = "6M";
    /// New Zealand Dollar.
    pub const FX_6N: &str = "6N";
    /// Micro GBP/USD.
    pub const FX_M6B: &str = "M6B";

    // CME agricultural futures.
    /// Lean Hogs.
    pub const HE: &str = "HE";
    /// Live Cattle.
    pub const LE: &str = "LE";
    /// Feeder Cattle.
    pub const GF: &str = "GF";

    // CME NYMEX futures.
    /// Crude Oil.
    pub const CL: &str = "CL";
    /// E-mini Crude Oil.
    pub const QM: &str = "QM";
    /// Natural Gas.
    pub const NG: &str = "NG";
    /// E-mini Natural Gas.
    pub const QG: &str = "QG";
    /// Micro Crude Oil.
    pub const MCL: &str = "MCL";
    /// RBOB Gasoline.
    pub const RB: &str = "RB";
    /// Heating Oil.
    pub const HO: &str = "HO";
    /// Platinum.
    pub const PL: &str = "PL";
    /// Micro Henry Hub Natural Gas.
    pub const MNG: &str = "MNG";

    // CME CBOT agricultural futures.
    /// Corn.
    pub const ZC: &str = "ZC";
    /// Wheat.
    pub const ZW: &str = "ZW";
    /// Soybeans.
    pub const ZS: &str = "ZS";
    /// Soybean Meal.
    pub const ZM: &str = "ZM";
    /// Soybean Oil.
    pub const ZL: &str = "ZL";

    // CME CBOT financial / interest rate futures.
    /// 2-Year Treasury Note.
    pub const ZT: &str = "ZT";
    /// 5-Year Treasury Note.
    pub const ZF: &str = "ZF";
    /// Treasury Bonds.
    pub const ZB: &str = "ZB";
    /// 10-Year Treasury Notes.
    pub const ZN: &str = "ZN";
    /// 10-Year Ultra Note.
    pub const TN: &str = "TN";
    /// Ultra Bond.
    pub const UB: &str = "UB";

    // CME COMEX futures.
    /// Gold.
    pub const GC: &str = "GC";
    /// Silver.
    pub const SI: &str = "SI";
    /// Copper.
    pub const HG: &str = "HG";
    /// Micro Gold.
    pub const MGC: &str = "MGC";
    /// Micro Silver.
    pub const SIL: &str = "SIL";
    /// Micro Copper.
    pub const MHG: &str = "MHG";

    /// Hard-coded supported CME-scoped futures roots for catalog and live requests.
    ///
    /// `MYM` is included here because some Rithmic systems expose it through CME routing.
    pub const SUPPORTED_CME_ROOTS: &[&str] = &[
        ES, MES, NQ, MNQ, RTY, M2K, NKD, EMD, MYM, MBT, MET, FX_6A, FX_6B, FX_6C, FX_6E, FX_6J,
        FX_6S, FX_E7, FX_M6E, FX_M6A, FX_6M, FX_6N, FX_M6B, HE, LE, GF,
    ];
    /// Hard-coded supported CBOT-scoped futures roots for catalog and live requests.
    ///
    /// `MYM` is included here because some Rithmic systems expose it through CBOT routing.
    pub const SUPPORTED_CBOT_ROOTS: &[&str] =
        &[YM, MYM, ZC, ZW, ZS, ZM, ZL, ZT, ZF, ZN, TN, ZB, UB];
    /// Hard-coded supported NYMEX-scoped futures roots for catalog and live requests.
    pub const SUPPORTED_NYMEX_ROOTS: &[&str] = &[CL, QM, NG, QG, MCL, RB, HO, PL, MNG];
    /// Hard-coded supported COMEX-scoped futures roots for catalog and live requests.
    pub const SUPPORTED_COMEX_ROOTS: &[&str] = &[GC, SI, HG, MGC, SIL, MHG];
}

/// Rithmic plant types (connection endpoints).
pub mod plants {
    /// Ticker plant for market data.
    pub const TICKER: &str = "ticker";
    /// Order plant for order management.
    pub const ORDER: &str = "order";
    /// PnL plant for account/position data.
    pub const PNL: &str = "pnl";
    /// History plant for historical data.
    pub const HISTORY: &str = "history";
}

#[cfg(test)]
mod tests {
    use super::products;

    #[rstest::rstest]
    fn requested_cme_equity_roots_are_represented() {
        assert_eq!(products::ES, "ES");
        assert_eq!(products::NKD, "NKD");
        assert_eq!(products::NQ, "NQ");
        assert_eq!(products::EMD, "EMD");
        assert_eq!(products::RTY, "RTY");
        assert_eq!(products::YM, "YM");
        assert_eq!(products::MES, "MES");
        assert_eq!(products::MYM, "MYM");
        assert_eq!(products::MNQ, "MNQ");
        assert_eq!(products::M2K, "M2K");
        assert_eq!(products::MBT, "MBT");
        assert_eq!(products::MET, "MET");
    }

    #[rstest::rstest]
    fn requested_cme_fx_roots_are_represented() {
        assert_eq!(products::FX_6A, "6A");
        assert_eq!(products::FX_6B, "6B");
        assert_eq!(products::FX_6C, "6C");
        assert_eq!(products::FX_6E, "6E");
        assert_eq!(products::FX_6J, "6J");
        assert_eq!(products::FX_6S, "6S");
        assert_eq!(products::FX_E7, "E7");
        assert_eq!(products::FX_M6E, "M6E");
        assert_eq!(products::FX_M6A, "M6A");
        assert_eq!(products::FX_6M, "6M");
        assert_eq!(products::FX_6N, "6N");
        assert_eq!(products::FX_M6B, "M6B");
    }

    #[rstest::rstest]
    fn requested_energy_ag_and_metals_roots_are_represented() {
        assert_eq!(products::HE, "HE");
        assert_eq!(products::LE, "LE");
        assert_eq!(products::GF, "GF");
        assert_eq!(products::ZC, "ZC");
        assert_eq!(products::ZW, "ZW");
        assert_eq!(products::ZS, "ZS");
        assert_eq!(products::ZM, "ZM");
        assert_eq!(products::ZL, "ZL");
        assert_eq!(products::CL, "CL");
        assert_eq!(products::QM, "QM");
        assert_eq!(products::NG, "NG");
        assert_eq!(products::QG, "QG");
        assert_eq!(products::HO, "HO");
        assert_eq!(products::RB, "RB");
        assert_eq!(products::MCL, "MCL");
        assert_eq!(products::ZT, "ZT");
        assert_eq!(products::ZF, "ZF");
        assert_eq!(products::ZN, "ZN");
        assert_eq!(products::TN, "TN");
        assert_eq!(products::ZB, "ZB");
        assert_eq!(products::UB, "UB");
        assert_eq!(products::GC, "GC");
        assert_eq!(products::SI, "SI");
        assert_eq!(products::HG, "HG");
        assert_eq!(products::MGC, "MGC");
        assert_eq!(products::SIL, "SIL");
        assert_eq!(products::MHG, "MHG");
    }

    #[rstest::rstest]
    fn known_exchanges_match_supported_catalog_scope() {
        use super::exchanges::{CBOT, CME, COMEX, KNOWN_EXCHANGES, NYMEX};

        assert_eq!(KNOWN_EXCHANGES, &[CME, CBOT, NYMEX, COMEX]);
    }

    #[rstest::rstest]
    fn supported_root_groups_match_expected_scope() {
        assert!(products::SUPPORTED_CME_ROOTS.contains(&products::MYM));
        assert!(products::SUPPORTED_CBOT_ROOTS.contains(&products::MYM));
        assert!(!products::SUPPORTED_CME_ROOTS.contains(&"FESX"));
        assert!(!products::SUPPORTED_CBOT_ROOTS.contains(&"FGBL"));
        assert!(!products::SUPPORTED_NYMEX_ROOTS.contains(&"BZ"));
        assert!(!products::SUPPORTED_COMEX_ROOTS.contains(&"PA"));
    }
}
