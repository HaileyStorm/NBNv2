using System.Collections.Generic;

namespace Nbn.Shared.Validation;

public sealed class NbnValidationResult
{
    private readonly List<NbnValidationIssue> _issues = new();

    public IReadOnlyList<NbnValidationIssue> Issues => _issues;

    public bool IsValid => _issues.Count == 0;

    public void Add(string message, string? context = null)
    {
        _issues.Add(new NbnValidationIssue(message, context));
    }
}

public readonly struct NbnValidationIssue
{
    public NbnValidationIssue(string message, string? context)
    {
        Message = message;
        Context = context;
    }

    public string Message { get; }
    public string? Context { get; }

    public override string ToString()
    {
        return Context is null ? Message : $"{Message} ({Context})";
    }
}