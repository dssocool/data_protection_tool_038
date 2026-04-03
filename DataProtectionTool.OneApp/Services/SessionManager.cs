using System.Collections.Concurrent;

namespace DataProtectionTool.OneApp.Services;

public record SessionInfo(string Oid, string Tid, string UserName, DateTime ConnectedAt);

public class SessionManager
{
    private readonly ConcurrentDictionary<string, SessionInfo> _sessions = new();

    public string Register(string oid, string tid, string userName)
    {
        var path = Guid.NewGuid().ToString("N");
        _sessions[path] = new SessionInfo(oid, tid, userName, DateTime.UtcNow);
        return path;
    }

    public bool TryGet(string path, out SessionInfo? info)
    {
        return _sessions.TryGetValue(path, out info);
    }
}
