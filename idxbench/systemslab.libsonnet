{
    local systemslab = self,

    barrier(name, id=null)::
        std.prune({
            uses: 'barrier',
            id: id,
            with: { name: name },
        }),

    shell(shell, command, id=null, background=null)::
        std.prune({
            uses: 'shell',
            id: id,
            background: background,
            with: {
                shell: shell,
                run: command,
            },
        }),

    bash(command, id=null, background=null)::
        systemslab.shell('bash', command, id, background),

    upload_artifact(path, name=null, id=null)::
        std.prune({
            uses: 'upload-artifact',
            id: id,
            with: {
                path: path,
                name: name,
            },
        }),

    write_file(path, content, id=null)::
        std.prune({
            uses: 'write-file',
            id: id,
            with: {
                path: path,
                content: content,
            },
        }),
}
