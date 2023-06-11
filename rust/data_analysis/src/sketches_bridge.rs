#[cxx::bridge]
pub mod ffi {

    #[derive(Default)]
    pub struct frequent_item {
        value: String,
        count: usize,
    }
    #[derive(Default)]
    pub struct frequent_item_with_range {
        value: String,
        count_min: usize,
        count_max: usize,
    }

    unsafe extern "C++" {
        include!("data_analysis/src/space_saving_bridge.hpp");

        type space_saving_sketch_t;

        pub fn new_space_saving_sketch(epsilon: f64) -> SharedPtr<space_saving_sketch_t>;

        pub fn sss_add_element(sss: SharedPtr<space_saving_sketch_t>, value: String);

        pub fn sss_clear(sss: SharedPtr<space_saving_sketch_t>);

        pub fn sss_size(sss: SharedPtr<space_saving_sketch_t>) -> usize;

        pub fn sss_guaranteed_frequent_items(
            sss: SharedPtr<space_saving_sketch_t>,
        ) -> UniquePtr<CxxVector<frequent_item>>;

        pub fn sss_frequent_items(
            sss: SharedPtr<space_saving_sketch_t>,
        ) -> UniquePtr<CxxVector<frequent_item>>;

        pub fn sss_frequent_items_with_range(
            sss: SharedPtr<space_saving_sketch_t>,
        ) -> UniquePtr<CxxVector<frequent_item_with_range>>;

        pub fn sss_combine(
            sss: SharedPtr<space_saving_sketch_t>,
            sss_other: SharedPtr<space_saving_sketch_t>,
        );

    }
}
