using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Row shape for Logic App varSlowTables and FormatSlowTablesHtml API.
/// </summary>
public class SlowTableEmailRow
{
    [JsonPropertyName("database")]
    public string? Database { get; set; }

    [JsonPropertyName("tableName")]
    public string? TableName { get; set; }

    [JsonPropertyName("partitionName")]
    public string? PartitionName { get; set; }

    [JsonPropertyName("processingTimeSeconds")]
    [JsonConverter(typeof(NullableFlexibleDoubleConverter))]
    public double? ProcessingTimeSeconds { get; set; }

    [JsonPropertyName("rowCount")]
    [JsonConverter(typeof(NullableFlexibleLongConverter))]
    public long? RowCount { get; set; }

    [JsonPropertyName("severity")]
    public string? Severity { get; set; }
}

internal sealed class NullableFlexibleDoubleConverter : JsonConverter<double?>
{
    public override double? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        if (reader.TokenType == JsonTokenType.Number && reader.TryGetDouble(out var numericValue))
            return numericValue;

        if (reader.TokenType == JsonTokenType.String)
        {
            var raw = reader.GetString();
            if (string.IsNullOrWhiteSpace(raw))
                return null;

            if (double.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var invariantValue))
                return invariantValue;

            if (double.TryParse(raw, NumberStyles.Any, CultureInfo.CurrentCulture, out var currentCultureValue))
                return currentCultureValue;
        }

        throw new JsonException("Unable to parse nullable double value.");
    }

    public override void Write(Utf8JsonWriter writer, double? value, JsonSerializerOptions options)
    {
        if (value.HasValue)
            writer.WriteNumberValue(value.Value);
        else
            writer.WriteNullValue();
    }
}

internal sealed class NullableFlexibleLongConverter : JsonConverter<long?>
{
    public override long? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var numericValue))
            return numericValue;

        if (reader.TokenType == JsonTokenType.String)
        {
            var raw = reader.GetString();
            if (string.IsNullOrWhiteSpace(raw))
                return null;

            if (long.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var invariantValue))
                return invariantValue;

            if (long.TryParse(raw, NumberStyles.Any, CultureInfo.CurrentCulture, out var currentCultureValue))
                return currentCultureValue;
        }

        throw new JsonException("Unable to parse nullable long value.");
    }

    public override void Write(Utf8JsonWriter writer, long? value, JsonSerializerOptions options)
    {
        if (value.HasValue)
            writer.WriteNumberValue(value.Value);
        else
            writer.WriteNullValue();
    }
}
