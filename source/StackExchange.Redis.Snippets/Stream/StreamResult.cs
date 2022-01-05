namespace StackExchange.Redis.Snippets.Stream;

public record StreamResult
{
    public RedisValue Id { get; init; }

    public NameValueEntry[]? Values { get; init; }

    public bool IsNull => Id == RedisValue.Null && Values == null;

    public RedisValue this[RedisValue fieldName]
    {
        get
        {
            var values = Values;
            if (values != null)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    if (values[i].Name == fieldName)
                        return values[i].Value;
                }
            }

            return RedisValue.Null;
        }
    }
}
