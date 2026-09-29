package forge

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

//go:embed testdata/forge-device-inventory-resource-convergence-v1.json
var forgeDeviceInventoryResourceConvergenceFixture []byte

func decodeForgeDeviceInventoryResourceConvergence(raw []byte) (map[string]any, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return nil, err
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func exactForgeDeviceInventoryResourceObject(value any, keys ...string) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok || len(object) != len(keys) {
		return nil, fmt.Errorf("object shape is not exact")
	}
	for _, key := range keys {
		if _, ok := object[key]; !ok {
			return nil, fmt.Errorf("missing field %q", key)
		}
	}
	return object, nil
}

func forgeDeviceInventoryResourceBool(object map[string]any, key string, want bool) error {
	value, ok := object[key].(bool)
	if !ok || value != want {
		return fmt.Errorf("field %q has unexpected boolean", key)
	}
	return nil
}

func forgeDeviceInventoryResourcePositive(object map[string]any, key string) error {
	value, ok := object[key].(float64)
	if !ok || value <= 0 {
		return fmt.Errorf("field %q is not positive", key)
	}
	return nil
}

func validateForgeDeviceInventoryResourceAuthority(value any) error {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"inventory_authoritative", "device_identity_verified",
		"reservation_created", "lease_issued", "execution_authorized",
		"dispatch_performed", "audit_published")
	if err != nil {
		return err
	}
	for key := range object {
		if err := forgeDeviceInventoryResourceBool(object, key, false); err != nil {
			return err
		}
	}
	return nil
}

func validateForgeClientInstanceResourceAuthority(value any) error {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"owner_authenticated", "session_read_authorized",
		"prompt_write_authorized", "device_identity_verified",
		"reservation_created", "execution_authorized",
		"dispatch_performed", "audit_published")
	if err != nil {
		return err
	}
	for key := range object {
		if err := forgeDeviceInventoryResourceBool(object, key, false); err != nil {
			return err
		}
	}
	return nil
}

func validateForgeDeviceInventoryResourceDevice(value any, owner any) (map[string]any, error) {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"device_id", "runner_instance_id", "owner", "revision", "generation",
		"heartbeat_sequence", "observed_at_ms", "approval_state", "cordon_state",
		"reservation_state", "liveness", "os", "architecture", "cpu_cores",
		"available_cpu_cores", "memory_bytes", "available_memory_bytes",
		"storage_bytes", "available_storage_bytes", "gpu_count",
		"available_gpu_memory_bytes")
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(object["owner"], owner) {
		return nil, fmt.Errorf("resource device owner drifted")
	}
	for _, key := range []string{"revision", "generation", "heartbeat_sequence", "observed_at_ms", "cpu_cores", "memory_bytes", "storage_bytes"} {
		if err := forgeDeviceInventoryResourcePositive(object, key); err != nil {
			return nil, err
		}
	}
	for _, key := range []string{"device_id", "runner_instance_id", "approval_state", "cordon_state", "reservation_state", "liveness", "os", "architecture"} {
		if value, ok := object[key].(string); !ok || value == "" {
			return nil, fmt.Errorf("field %q is not a non-empty string", key)
		}
	}
	return object, nil
}

func validateForgeDeviceInventoryObservationDevice(value any, owner any) (map[string]any, error) {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"device_id", "owner", "approval_state", "cordon_state",
		"reservation_state", "liveness", "snapshot_observed_at_ms",
		"lease_expires_at_ms", "os", "architecture", "available_cpu_cores",
		"available_memory_bytes", "available_storage_bytes", "runtimes", "gpus",
		"data_residency_zones", "trust_zone", "sandbox_levels",
		"concurrency_limit", "active_concurrency")
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(object["owner"], owner) {
		return nil, fmt.Errorf("inventory device owner drifted")
	}
	for _, key := range []string{"device_id", "approval_state", "cordon_state", "reservation_state", "liveness", "os", "architecture", "trust_zone"} {
		if value, ok := object[key].(string); !ok || value == "" {
			return nil, fmt.Errorf("inventory field %q is not a non-empty string", key)
		}
	}
	for _, key := range []string{"snapshot_observed_at_ms", "lease_expires_at_ms"} {
		if err := forgeDeviceInventoryResourcePositive(object, key); err != nil {
			return nil, err
		}
	}
	for _, key := range []string{"available_cpu_cores", "available_memory_bytes", "available_storage_bytes", "concurrency_limit", "active_concurrency"} {
		if value, ok := object[key].(float64); !ok || value < 0 {
			return nil, fmt.Errorf("inventory field %q is not a non-negative number", key)
		}
	}
	if _, ok := object["runtimes"].([]any); !ok {
		return nil, fmt.Errorf("inventory runtimes are not an array")
	}
	if _, ok := object["gpus"].([]any); !ok {
		return nil, fmt.Errorf("inventory GPUs are not an array")
	}
	if _, ok := object["data_residency_zones"].([]any); !ok {
		return nil, fmt.Errorf("inventory residency zones are not an array")
	}
	if _, ok := object["sandbox_levels"].([]any); !ok {
		return nil, fmt.Errorf("inventory sandbox levels are not an array")
	}
	return object, nil
}

func validateForgeDeviceInventoryResourceInventory(value any) (map[string]any, map[string]any, error) {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"schema_version", "evaluation_mode", "evaluated_at_ms",
		"owner_declaration", "owner_declaration_unverified",
		"inventory_declarations_unverified", "notice", "devices",
		"execution_authorized", "reservation_created", "dispatch_performed")
	if err != nil {
		return nil, nil, err
	}
	if object["schema_version"] != "forge.device-inventory-observation/v2" ||
		object["evaluation_mode"] != "offline_static_only" {
		return nil, nil, fmt.Errorf("inventory schema drifted")
	}
	for _, key := range []string{"owner_declaration_unverified", "inventory_declarations_unverified", "execution_authorized", "reservation_created", "dispatch_performed"} {
		if err := forgeDeviceInventoryResourceBool(object, key, key == "owner_declaration_unverified" || key == "inventory_declarations_unverified"); err != nil {
			return nil, nil, err
		}
	}
	if _, err := exactForgeDeviceInventoryResourceObject(object["owner_declaration"], "issuer", "subject", "tenant_id"); err != nil {
		return nil, nil, err
	}
	devices, ok := object["devices"].([]any)
	if !ok || len(devices) != 1 {
		return nil, nil, fmt.Errorf("inventory device rows are not bounded")
	}
	candidate, ok := devices[0].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("inventory candidate is not an object")
	}
	device, ok := candidate["device"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("inventory device projection is not an object")
	}
	device, err = validateForgeDeviceInventoryObservationDevice(device, object["owner_declaration"])
	if err != nil {
		return nil, nil, err
	}
	candidate, err = exactForgeDeviceInventoryResourceObject(devices[0],
		"instance_id", "revision", "generation", "heartbeat_sequence", "device")
	if err != nil {
		return nil, nil, err
	}
	for _, key := range []string{"instance_id"} {
		if value, ok := candidate[key].(string); !ok || value == "" {
			return nil, nil, fmt.Errorf("inventory candidate %q is not a non-empty string", key)
		}
	}
	for _, key := range []string{"revision", "generation", "heartbeat_sequence"} {
		if err := forgeDeviceInventoryResourcePositive(candidate, key); err != nil {
			return nil, nil, err
		}
	}
	if !reflect.DeepEqual(candidate["device"], device) {
		return nil, nil, fmt.Errorf("inventory device projection drifted")
	}
	return object, candidate, nil
}
func validateForgeDeviceInventoryResourceView(value any, owner any) (map[string]any, error) {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"schema_version", "evaluation_mode", "owner_declaration",
		"owner_declaration_unverified", "instances", "devices",
		"device_attributes_unverified", "read_only", "authority")
	if err != nil {
		return nil, err
	}
	if object["schema_version"] != "forge.client-instance-resource-view/v1" ||
		object["evaluation_mode"] != "owner_bound_instance_resource_view_only" ||
		!reflect.DeepEqual(object["owner_declaration"], owner) {
		return nil, fmt.Errorf("resource view owner/schema drifted")
	}
	for _, key := range []string{"owner_declaration_unverified", "device_attributes_unverified", "read_only"} {
		if err := forgeDeviceInventoryResourceBool(object, key, true); err != nil {
			return nil, err
		}
	}
	if err := validateForgeClientInstanceResourceAuthority(object["authority"]); err != nil {
		return nil, err
	}
	instances, ok := object["instances"].([]any)
	if !ok || len(instances) == 0 {
		return nil, fmt.Errorf("resource view has no instance rows")
	}
	devices, ok := object["devices"].([]any)
	if !ok || len(devices) != 1 {
		return nil, fmt.Errorf("resource view device rows are not bounded")
	}
	if _, err := validateForgeDeviceInventoryResourceDevice(devices[0], owner); err != nil {
		return nil, err
	}
	return object, nil
}

func validateForgeDeviceInventoryResourceEnvelope(value map[string]any) error {
	object, err := exactForgeDeviceInventoryResourceObject(value,
		"schema_version", "evaluation_mode", "inventory", "resource_view",
		"converged", "read_only", "authority")
	if err != nil {
		return err
	}
	if object["schema_version"] != "forge.device-inventory-resource-convergence/v1" ||
		object["evaluation_mode"] != "owner_bound_inventory_resource_convergence_only" {
		return fmt.Errorf("convergence schema drifted")
	}
	if err := forgeDeviceInventoryResourceBool(object, "converged", true); err != nil {
		return err
	}
	if err := forgeDeviceInventoryResourceBool(object, "read_only", true); err != nil {
		return err
	}
	if err := validateForgeDeviceInventoryResourceAuthority(object["authority"]); err != nil {
		return err
	}
	inventory, device, err := validateForgeDeviceInventoryResourceInventory(object["inventory"])
	if err != nil {
		return err
	}
	resource, err := validateForgeDeviceInventoryResourceView(object["resource_view"], inventory["owner_declaration"])
	if err != nil {
		return err
	}
	resourceDevice, ok := resource["devices"].([]any)[0].(map[string]any)
	if !ok {
		return fmt.Errorf("resource device is not an object")
	}
	inventoryCandidate := device
	inventoryDevice, ok := inventoryCandidate["device"].(map[string]any)
	if !ok {
		return fmt.Errorf("inventory device is not an object")
	}
	joined := map[string]any{
		"device_id":          inventoryDevice["device_id"],
		"runner_instance_id": inventoryCandidate["instance_id"],
		"revision":           inventoryCandidate["revision"],
		"generation":         inventoryCandidate["generation"],
		"heartbeat_sequence": inventoryCandidate["heartbeat_sequence"],
	}
	for key, resourceKey := range map[string]string{
		"device_id": "device_id", "runner_instance_id": "runner_instance_id",
		"revision": "revision", "generation": "generation",
		"heartbeat_sequence": "heartbeat_sequence",
	} {
		if !reflect.DeepEqual(joined[key], resourceDevice[resourceKey]) {
			return fmt.Errorf("inventory/resource field %q drifted", key)
		}
	}
	return nil
}

func TestForgeDeviceInventoryResourceConvergenceCanonical(t *testing.T) {
	value, err := decodeForgeDeviceInventoryResourceConvergence(
		forgeDeviceInventoryResourceConvergenceFixture,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateForgeDeviceInventoryResourceEnvelope(value); err != nil {
		t.Fatalf("canonical inventory/resource envelope: %v", err)
	}
}

func TestForgeDeviceInventoryResourceConvergenceRejectsMutations(t *testing.T) {
	value, err := decodeForgeDeviceInventoryResourceConvergence(
		forgeDeviceInventoryResourceConvergenceFixture,
	)
	if err != nil {
		t.Fatal(err)
	}
	value["unexpected"] = true
	if err := validateForgeDeviceInventoryResourceEnvelope(value); err == nil {
		t.Fatal("unknown convergence field accepted")
	}
	value, _ = decodeForgeDeviceInventoryResourceConvergence(
		forgeDeviceInventoryResourceConvergenceFixture,
	)
	value["authority"].(map[string]any)["lease_issued"] = true
	if err := validateForgeDeviceInventoryResourceEnvelope(value); err == nil {
		t.Fatal("authority mutation accepted")
	}
	value, _ = decodeForgeDeviceInventoryResourceConvergence(
		forgeDeviceInventoryResourceConvergenceFixture,
	)
	value["resource_view"].(map[string]any)["devices"].([]any)[0].(map[string]any)["heartbeat_sequence"] = float64(5)
	if err := validateForgeDeviceInventoryResourceEnvelope(value); err == nil {
		t.Fatal("counter drift accepted")
	}
	duplicate := strings.TrimSuffix(strings.TrimSpace(string(forgeDeviceInventoryResourceConvergenceFixture)), "}") +
		fmt.Sprintf(",%q:%q}", "schema_version", "forge.device-inventory-resource-convergence/v1")
	if _, err := decodeForgeDeviceInventoryResourceConvergence([]byte(duplicate)); err == nil {
		t.Fatal("duplicate convergence field accepted")
	}
}
