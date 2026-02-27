"""Tests for EventAgent correlation configuration."""

from eventagent.config import Config, CorrelationConfig, default_config


def test_correlation_config_default():
    """Test CorrelationConfig defaults to None."""
    config = CorrelationConfig()
    assert config.key is None


def test_correlation_config_with_key():
    """Test CorrelationConfig with explicit key."""
    config = CorrelationConfig(key="order_id")
    assert config.key == "order_id"
    config = CorrelationConfig(key="customer_id")
    assert config.key == "customer_id"
    config = CorrelationConfig(key="payment_id")
    assert config.key == "payment_id"


def test_config_default():
    """Test Config has default correlation keys enabled."""
    config = Config()
    assert config.order_id is True
    assert config.customer_id is True
    assert config.payment_id is True
    assert config.correlation_keys == ["order_id", "customer_id", "payment_id"]


def test_config_multiple_correlation_keys_future():
    """Test correlation_keys property returns configured keys."""
    config = Config()
    # All three should be present by default
    assert "order_id" in config.correlation_keys
    assert "customer_id" in config.correlation_keys
    assert "payment_id" in config.correlation_keys


def test_default_config():
    """Test the global default_config is properly initialized."""
    assert default_config.order_id is True
    assert default_config.customer_id is True
    assert default_config.payment_id is True
    assert default_config.correlation_keys == ["order_id", "customer_id", "payment_id"]


def test_correlation_config_literal_type():
    """Test that only valid literal values are accepted."""
    # Valid values
    config = CorrelationConfig(key="order_id")
    assert config.key == "order_id"
    
    config = CorrelationConfig(key="customer_id")
    assert config.key == "customer_id"
    
    config = CorrelationConfig(key="payment_id")
    assert config.key == "payment_id"
    
    # None is also valid
    config = CorrelationConfig(key=None)
    assert config.key is None
    
    # Should raise validation error for invalid key
    try:
        CorrelationConfig(key="invalid_key")  # type: ignore
        assert False, "Should have raised validation error"
    except Exception:
        pass  # Expected - validation should fail


def test_config_disable_correlation_keys():
    """Test disabling specific correlation keys."""
    config = Config(order_id=False, customer_id=False, payment_id=False)
    assert config.correlation_keys == []
    
    config = Config(order_id=True, customer_id=False, payment_id=False)
    assert config.correlation_keys == ["order_id"]