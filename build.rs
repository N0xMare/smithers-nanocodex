fn main() {
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown-target".to_owned());
    println!("cargo:rustc-env=SMITHERS_NANOCODEX_TARGET={target}");
}
