
## Features in Detail

1. **Row Tracking**
   - Monitors additions and deletions of rows
   - Shows net change in row count

2. **Column Changes**
   - Identifies new columns
   - Tracks removed columns
   - Detects modified columns

3. **Schema Validation**
   - Monitors data type changes
   - Alerts on schema modifications

4. **Content Analysis**
   - Detailed before/after comparisons
   - Shows specific value changes
   - Includes context columns when available

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Authors

- Necromenta (andrezcar1998@gmail.com)



## Examples

```python

# As a decorator
@DataFrameChangeTracker.track_dataframe_changes
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # your transformation code
    return df

# Or directly compare two dataframes
df1 = pd.DataFrame(...)
df2 = pd.DataFrame(...)
DataFrameChangeTracker.analyze_dataframe_changes(df1, df2)
```
