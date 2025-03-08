fn main() {
    println!("cargo:rerun-if-changed=bin/");
    println!("cargo:rustc-link-search=native=bin/");
    println!("cargo:rustc-link-lib=static=admac");
}
