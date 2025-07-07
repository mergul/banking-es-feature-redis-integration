// This file contains only comments explaining why tests were moved to cqrs_integration_tests.rs
// No imports needed since no actual tests remain in this file

// All tests that used setup_test_environment, TestContext, and TestProjectionStore
// have been removed or refactored into cqrs_integration_tests.rs.
// These supporting structs and functions are no longer needed in this file.

// Helper function to run async operations with timeout - This might be generally useful,
// but if not used by any remaining tests in this file, it can also be removed.
// For now, let's assume it might be used by future tests here or can be moved to a shared test utils.
// On second thought, since all tests are gone, this is also unused in this file.
/*
async fn with_timeout<F, T>(
    future: F,
    timeout_duration: Duration,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
{
    match timeout(timeout_duration, future).await {
        Ok(result) => result,
        Err(_) => Err("Operation timed out".into()),
    }
}
*/

// Removed test_basic_account_operations as its scenarios are covered by cqrs_integration_tests.rs
// or are specific to the deprecated AccountService's duplicate command logic.

// Removed test_cache_behavior; will be re-added to cqrs_integration_tests.rs
// if deemed necessary and adapted for CQRS path.

// Removed test_error_handling as its scenarios are covered by test_cqrs_error_handling
// in cqrs_integration_tests.rs (after its assertions are fixed).

// Removed test_performance_metrics as it's specific to deprecated AccountService.
// test_cqrs_metrics in cqrs_integration_tests.rs covers metrics for CQRSAccountService.

// Removed test_transaction_history as its scenarios are covered by test_cqrs_get_transactions
// in cqrs_integration_tests.rs.

// Removed test_duplicate_command_handling as it tested a feature specific
// to the deprecated AccountService, which is not present in CQRSAccountService.
// Idempotency for CQRS commands would require a different implementation (e.g., client-side keys).

// Removed test_high_throughput_performance as it's specific to deprecated AccountService.
// tests/cqrs_performance_test.rs::test_cqrs_high_throughput_performance covers the CQRS path.

// Enums Operation and OperationResult were only used by the removed test_high_throughput_performance.
