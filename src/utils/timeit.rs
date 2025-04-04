#[macro_export]
macro_rules! timeit {
    // 基本表达式计时
    ($code:expr) => {
        $crate::_timeit_internal!("Anonymous code", $code)
    };

    // 代码块计时
    ($code:block) => {
        $crate::_timeit_internal!("Anonymous code", $code)
    };

    // 命名表达式计时
    ($name:literal, $code:expr) => {
        $crate::_timeit_internal!($name, $code)
    };

    // 命名代码块计时
    ($name:literal, $code:block) => {
        $crate::_timeit_internal!($name, $code)
    };
}

// 内部帮助宏处理共同逻辑
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
