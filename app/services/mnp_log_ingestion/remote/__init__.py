"""Remote (SSH/SFTP) pull-ingestion of M3 logs from the Windows Server(s).

A tenant can register one or more `LogSshSource` rows (each a Windows Server running OpenSSH).
This package connects over SFTP, reads the configured log dir, and feeds the bytes into the
existing Stage 1 ingest + Stage 2 finalize — nothing about parsing/grouping changes here.
"""
