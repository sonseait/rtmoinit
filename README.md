# rtmoinit

Thư viện dashboard realtime nhúng vào ứng dụng Go + Fiber v3. Ứng dụng tự định nghĩa dashboard bằng Go: stat, biểu đồ line/bar và bảng. Ghi dữ liệu vào RAM; Redis tùy chọn để khôi phục sau restart.

Go 1.27.4. Không cần frontend build, CDN, Prometheus hoặc server dashboard riêng.

## Chạy ví dụ

```sh
go mod download
go run ./examples/basic
```

Mở http://127.0.0.1:3000/monitor/. Ví dụ ghi dữ liệu giả mỗi 250 ms.

Bật checkpoint Redis:

```sh
REDIS_ADDR=127.0.0.1:6379 go run ./examples/basic
```

Có thể cấu hình `REDIS_USERNAME`, `REDIS_PASSWORD`, `LISTEN_ADDR`. Ví dụ mặc định chỉ nghe localhost. Dừng bằng Ctrl+C để checkpoint và shutdown.

## Tích hợp

Trước khi module được publish, thêm local replacement trong project sử dụng:

```sh
go mod edit -replace github.com/sonseait/rtmoinit=../rtmoinit
go get github.com/sonseait/rtmoinit
```

```go
import (
    "context"
    "time"

    "github.com/gofiber/fiber/v3"
    "github.com/redis/go-redis/v9"
    "github.com/sonseait/rtmoinit"
)

client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    ContextTimeoutEnabled: true,
    MaxRetries: -1,
    DialTimeout: 3 * time.Second,
})
// Ứng dụng đóng client sau khi monitor.Close hoàn tất.

monitor, err := rtmoinit.New(context.Background(), rtmoinit.Config{
    Redis: client, // bỏ trường này để chỉ dùng RAM
    Namespace: "my-service-instance-1",
    Definition: rtmoinit.Definition{
        Series: []rtmoinit.Series{
            {ID: "requests", Title: "Requests", Kind: rtmoinit.Counter},
            {ID: "latency", Title: "Latency", Unit: "ms", Kind: rtmoinit.Gauge},
        },
        Dashboards: []rtmoinit.Dashboard{{
            ID: "overview", Title: "Overview",
            Panels: []rtmoinit.Panel{
                {ID: "requests", Title: "Requests", Type: rtmoinit.Stat, Series: []string{"requests"}},
                {ID: "latency", Title: "Latency", Type: rtmoinit.Line, Series: []string{"latency"}},
            },
        }},
    },
})
if err != nil {
    return err
}

app := fiber.New()
monitor.Mount(app.Group("/monitor", yourAuthMiddleware))

if err := monitor.Add("requests", 1); err != nil {
    return err
}
if err := monitor.Set("latency", 42.5); err != nil {
    return err
}
```

`yourAuthMiddleware` là middleware xác thực của ứng dụng. Định nghĩa toàn bộ dashboard trong `Config.Definition` trước `New` để kiểm tra và restore cùng một schema. Không có đăng ký động sau startup.

- `Set` dành cho gauge; `Add` dành cho counter, delta không âm. Counter tiếp tục từ checkpoint đã restore.
- Giá trị số dùng `float64`, phục vụ hiển thị; không dùng làm sổ cái hoặc lưu số nguyên cần chính xác ngoài phạm vi IEEE 754.
- `SetRows(tableID, []rtmoinit.Row)` thay toàn bộ bảng. Khai báo cột trong `Definition.Tables`, rồi dùng panel `TablePanel` với trường `Table`. Cell nhận string, bool, số hữu hạn hoặc nil; không nhận object/array. Số nguyên được chuẩn hóa sang `float64`.
- ID dài 1–128 ký tự, gồm chữ/số và `_.-`, bắt đầu bằng chữ/số. Series/table dùng ID toàn cục; panel ID duy nhất trong từng dashboard.
- Panel `Width` là 1–12 cột, mặc định 6; trên mobile luôn đầy chiều ngang. Stat tham chiếu đúng một series; line/bar có thể chứa nhiều series.
- Dữ liệu đầu vào được sao chép. `Snapshot()` trả bản sao có thể đọc/sửa độc lập. Snapshot không phải transaction giữa nhiều series.
- Ứng dụng xử lý lỗi cập nhật; `errors.Is` nhận diện `ErrInvalid`, `ErrLimit`, `ErrClosed`.

Ví dụ đầy đủ có bảng, line, bar và shutdown: [examples/basic/main.go](examples/basic/main.go).

## RAM, realtime và giới hạn

| Cấu hình | Mặc định |
| --- | --- |
| `History` | 1 giờ |
| `SampleInterval` | 1 giây |
| `FlushInterval` | 10 giây |
| `FlushTimeout` | 3 giây, gồm thời gian chờ slot checkpoint |
| `MaxSeries` | 100 |
| `MaxRows` | 1.000 dòng/bảng |
| `MaxCheckpointBytes` | 10 MiB, sau gzip |
| `MaxClients` | 100 kết nối SSE |
| `Namespace` | `rtmoinit` |

Giá trị zero dùng mặc định; giá trị âm bị từ chối. Ring buffer mỗi series giữ tối đa `History / SampleInterval` mẫu, làm tròn xuống. Gauge lấy giá trị cuối; counter lấy tổng tích lũy, không tự chuyển thành rate. Chưa có dữ liệu thì stat hiển thị `—`. Không nội suy khi process ngừng chạy.

Tổng JSON bảng tối đa bằng nửa `MaxCheckpointBytes`. Quá giới hạn dòng/byte sẽ từ chối cập nhật và giữ dữ liệu trước đó. Checkpoint vượt giới hạn nén hoặc 16 lần giới hạn sau giải nén sẽ bị từ chối, giữ checkpoint đã lưu; lỗi hiển thị trên UI/status. Điều chỉnh giới hạn cùng thời gian giữ khi số lượng series hoặc dữ liệu tăng.

SSE cập nhật mỗi giây. Kết nối mới nhận snapshot đầy đủ; các lần sau nhận cửa sổ mẫu chồng lặp ngắn để ghép lịch sử. Client chậm hoặc reconnect nhận lại snapshot đầy đủ, không replay event log. Bản update được serialize dùng chung giữa các client; kết nối có thời hạn ghi 5 giây. Bảng vẫn truyền toàn bộ snapshot mới nhất, phù hợp bảng giám sát nhỏ.

UI dùng plain HTML/CSS/JavaScript và SVG được nhúng trong binary. Chuỗi thời gian hiển thị theo giờ trình duyệt. Biểu đồ bar hiển thị tối đa 120 cột/series, chọn một mẫu mỗi nhóm; không tự tính tổng nhóm.

## Checkpoint Redis

Luồng ghi `Set`/`Add` chỉ cập nhật RAM. Sampling và persistence là hai worker độc lập. Worker checkpoint sao chép từng series dưới khóa riêng, sau đó encode JSON và nén gzip ở ngoài khóa; dùng một lệnh `SET` thay thế nguyên tử key `<namespace>:checkpoint`, không đặt TTL.

Chỉ một checkpoint hoạt động tại một thời điểm. Worker không tạo hàng đợi khi Redis chậm. `Flush(ctx)` cho phép checkpoint thủ công, chờ slot trong deadline. Không có Redis thì `Flush` không làm gì.

Client Redis phải bật `ContextTimeoutEnabled: true`. Khuyến nghị client/pool riêng với `MaxRetries: -1` và dial timeout hữu hạn để checkpoint không tranh connection với luồng nghiệp vụ. Redis server riêng hữu ích khi cần cách ly CPU/RAM/eviction; DB index khác vẫn dùng chung tài nguyên server.

- Startup: Redis chưa có key thì bắt đầu rỗng. Không đọc được, checkpoint hỏng, khác version hoặc khác định nghĩa/thời gian lấy mẫu thì `New` trả lỗi, không ghi đè dữ liệu đã lưu.
- Runtime: Redis lỗi vẫn tiếp tục ghi RAM, thử lại ở chu kỳ sau. `Status()` và `/status` báo lỗi, lần thử, thời gian ghi và lần thành công gần nhất.
- Shutdown: dừng các goroutine của ứng dụng đang cập nhật, gọi `Close(ctx)`, rồi mới đóng Redis. `Close` từ chối ghi mới, đợi checkpoint đang chạy và thử ghi lần cuối. Nên dành ít nhất `2 * FlushTimeout` cho shutdown. Context hết hạn chỉ dừng chờ; cleanup tiếp tục trong nền.
- Thay đổi definition: đổi namespace để bắt đầu mới; hoặc chủ động xóa key checkpoint cũ nếu chấp nhận mất lịch sử. Thư viện không tự migrate hay xóa dữ liệu.

Mỗi namespace chỉ có **một instance đang ghi**. Không hỗ trợ distributed aggregation hoặc nhiều writer cùng key. Checkpoint có thể mất phần RAM chưa lưu khi crash; Redis lỗi kéo dài làm khoảng dữ liệu có thể mất tăng lên. Timeout sau khi gửi `SET` có thể có kết quả không xác định: Redis có thể đã nhận snapshot đầy đủ. Độ bền khi Redis restart phụ thuộc cấu hình AOF/RDB và storage của Redis.

## HTTP

Tất cả route tương đối với group truyền vào `Mount`:

| Method | Route | Nội dung |
| --- | --- | --- |
| GET | `/` | UI nhúng |
| GET | `/definition` | Dashboard/series/table cùng `historyMs`, `sampleIntervalMs` |
| GET | `/snapshot` | Giá trị, lịch sử, bảng và trạng thái checkpoint |
| GET | `/status` | Trạng thái checkpoint |
| GET | `/events` | SSE `snapshot` đầy đủ hoặc `update` cửa sổ ngắn |

Mẫu lịch sử là `[unixMilliseconds, value]`. `update` chứa giá trị hiện tại của mọi series, lịch sử ngắn và toàn bộ bảng; ghép mẫu theo timestamp và loại lịch sử quá `historyMs`. UI sẵn có thực hiện việc ghép này.

Mount một lần trước khi server chạy. Gắn xác thực trên group để bảo vệ **toàn bộ** route; thư viện không có tài khoản riêng hay remote write API. EventSource dùng cookie cùng origin. Nếu reverse proxy có buffer/compression, tắt chúng cho SSE và đặt read timeout dài hơn chu kỳ update.

## Kiểm tra

```sh
go mod download
gofmt -w .
test -z "$(gofmt -l .)"
go vet ./...
go test -race ./...
go build ./...
go test -run '^$' -bench BenchmarkAdd -benchmem
```

Test Redis tự skip nếu chưa đặt địa chỉ. Chỉ dùng Redis test riêng vì suite có `CLIENT PAUSE`; không chạy vào Redis nghiệp vụ:

```sh
RTMOINIT_TEST_REDIS_ADDR=127.0.0.1:6379 go test -race ./...
```
