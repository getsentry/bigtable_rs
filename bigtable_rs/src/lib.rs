//!
//! A simple Google Bigtable client.
//!
//! See [`bigtable`] and [`bigtable_table_admin`] packages for more info.
//!
//! [[github repo]]
//!
//! [`bigtable`]: mod@crate::bigtable
//! [github repo]: https://github.com/liufuyang/bigtable_rs
mod auth_service;
pub mod bigtable;
pub mod bigtable_table_admin;
pub mod google;
mod root_ca_certificate;
pub mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
