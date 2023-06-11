// build.rs

fn main() {
    cxx_build::bridge("src/sketches_bridge.rs") // returns a cc::Build
        .flag_if_supported("-std=c++14")
        .flag_if_supported("-DNDEBUG")
        .file("src/space_saving_bridge.cpp")
        .compile("data_analysis");
}
