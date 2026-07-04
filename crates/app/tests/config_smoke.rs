//! Smoke test: the SHIPPED live configs parse AND pass validation. `from_toml_str`
//! both deserializes (with `deny_unknown_fields`) and runs the semantic validator,
//! so this guards a typo / out-of-range value in a config the bot actually runs
//! with (e.g. the dynamic `gross_pct` copy caps in the copy canary).

#[test]
fn copy_canary_config_is_valid() {
    let src = include_str!("../../../mm-live-copy-canary.toml");
    let cfg = pm_config::Config::from_toml_str(src)
        .expect("mm-live-copy-canary.toml must parse and validate");
    // Dynamic equity-scaled caps are wired: gross_pct in (0, 1], copy enabled+live.
    assert!(
        cfg.copy_params.gross_pct > 0.0 && cfg.copy_params.gross_pct <= 1.0,
        "copy canary should use dynamic gross_pct sizing"
    );
    assert!(cfg.strategies.copy.enabled && cfg.strategies.copy.live);
}
