package thingspanel

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"gopkg.in/redis.v5"
)

func TestDeviceAuthCacheUsesVoucherAndDeviceID(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	previousRedis, previousDB := redisCache, db
	redisCache = redis.NewClient(&redis.Options{Addr: server.Addr()})
	db = nil
	t.Cleanup(func() {
		_ = redisCache.Close()
		redisCache, db = previousRedis, previousDB
	})
	device := &Device{ID: "device-id", Voucher: `{"username":"device-user","password":"device-pass"}`, DeviceNumber: "NSNR-001"}
	if err = cacheDevice(device.Voucher, device); err != nil {
		t.Fatal(err)
	}
	if cachedID, _ := server.Get(device.Voucher); cachedID != device.ID {
		t.Fatalf("voucher cache points to %q, want %q", cachedID, device.ID)
	}
	if _, err = server.Get(device.ID); err != nil {
		t.Fatalf("device JSON was not cached under the device ID: %v", err)
	}
	cached, err := GetDeviceById(device.ID)
	if err != nil || cached.ID != device.ID || cached.DeviceNumber != device.DeviceNumber {
		t.Fatalf("Redis device cache miss: device=%#v err=%v", cached, err)
	}
}
