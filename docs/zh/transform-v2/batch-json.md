# BatchJson

> 按固定数量聚合数据，并按照模板封装为嵌套 JSON 对象。

## 配置项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| batch.size | int | 是 | - | 每个完整批次包含的输入数据条数，必须大于 0 |
| batch.fields | list | 否 | `[]` | `${batch.data}` 内的字段选择、顺序、改名和类型转换规则 |
| json.template | object | 是 | - | 完整的输出对象模板，必须包含且只能包含一个 `"${batch.data}"` 占位符 |

`"${batch.data}"` 表示当前批次的数据数组。它可以放在模板中的任意属性和任意对象层级，输出属性顺序与模板顺序一致。

## 配置示例

```hocon
transform {
  BatchJson {
    plugin_input = ["source_data"]
    plugin_output = "batched_data"

    batch.size = 3
    json.template = {
      data = "${batch.data}"
      status = "active"
      metadata = {
        source = "mysql"
        version = "1.0"
      }
      request_info = {
        request_id = "a1b2c3d4"
        timestamp = "2026-08-19 10:00:00"
      }
    }
  }
}
```

如果批量数据需要放在更深层级，只需移动占位符：

```hocon
json.template = {
  status = "active"
  response = {
    metadata = {
      source = "mysql"
      version = "1.0"
    }
    records = "${batch.data}"
  }
  request_id = "a1b2c3d4"
}
```

此时批量数据会出现在 `response.records` 中。

三条输入数据会生成一个输出对象：

```json
{
  "data": [
    {"id": 1, "name": "张三", "age": 25},
    {"id": 2, "name": "李四", "age": 30},
    {"id": 3, "name": "王五", "age": 28}
  ],
  "status": "active",
  "metadata": {
    "source": "mysql",
    "version": "1.0"
  },
  "request_info": {
    "request_id": "a1b2c3d4",
    "timestamp": "2026-08-19 10:00:00"
  }
}
```

## 批次字段改名和类型转换

配置 `batch.fields` 后，列表中的字段会按配置顺序放入 `${batch.data}`。每条规则支持：

- `source`：源字段名，必填。
- `target`：输出字段名，可不填；不填时沿用源字段名。
- `type`：输出字段类型，可不填；不填时沿用源字段类型。

```hocon
batch.fields = [
  { source = "id",   target = "user_id",   type = "STRING" }
  { source = "name", target = "user_name" }
  { source = "age",  target = "user_age",  type = "BIGINT" }
]
```

输入：

```json
{"id": 1, "name": "张三", "age": 25}
```

`${batch.data}` 中对应的数据变为：

```json
{"user_id": "1", "user_name": "张三", "user_age": 25}
```

不配置 `batch.fields` 时，所有源字段保持原名称、原类型和原顺序，兼容原有配置。配置后，只保留列表中声明的字段。

支持的目标类型包括 `STRING`、`BOOLEAN`、`TINYINT`、`SMALLINT`、`INT`、`BIGINT`、`FLOAT`、`DOUBLE`、`DECIMAL(p,s)`、`BYTES`、`DATE`、`TIME`、`TIMESTAMP` 和 `TIMESTAMP_TZ`。

如果字段不存在、输出字段重名或类型无效，任务会在启动时失败。如果某个实际值无法转换，任务会明确报告源字段、输出字段、目标类型、原始值和原始值类型，不会悄悄生成错误数据。

## 批次结束与异常恢复

- 有界任务结束时，最后不足 `batch.size` 的数据仍会封装成一批并发送给 Sink。
- 检查点会保存尚未凑满一批的数据；任务异常恢复后，会从该批未完成的位置继续处理。
- `batch.size` 按每个并行任务独立计数。若必须严格按全局顺序分批，请将上游并行度设置为 1。
- `json.template` 中必须恰好有一个 `"${batch.data}"` 占位符，否则任务会在启动时直接报错。
