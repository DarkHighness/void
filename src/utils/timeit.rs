#[macro_export]
macro_rules! timeit {
    ($code:expr) => {
        $crate::_timeit_internal!("Anonymous code", $code)
    };

    ($code:block) => {
        $crate::_timeit_internal!("Anonymous code", $code)
    };

    ($name:literal, $code:expr) => {
        $crate::_timeit_internal!($name, $code)
    };

    ($name:literal, $code:block) => {
        $crate::_timeit_internal!($name, $code)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! _timeit_internal {
    ($label:expr, $code:expr) => {{
        let start = std::time::Instant::now();
        let result = $code;
        let duration = start.elapsed();

        log::info!(
            "{} took {}.{:04} seconds",
            $label,
            duration.as_secs(),
            duration.subsec_millis()
        );

        result
    }};
}
