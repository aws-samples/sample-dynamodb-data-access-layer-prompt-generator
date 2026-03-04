# Security Policy

## Supported Versions

We release patches for security vulnerabilities. The following versions are currently supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 2.0.x   | :white_check_mark: |
| < 2.0   | :x:                |

## Reporting a Vulnerability

We take the security of DAL Prompt Generator seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via:

1. **GitHub Security Advisories**: Use the "Security" tab in the repository to privately report a vulnerability
2. **Email**: Contact the maintainers directly through the project's contact information

### What to Include

Please include the following information in your report:

- Type of vulnerability (e.g., code injection, path traversal, etc.)
- Full paths of source file(s) related to the vulnerability
- Location of the affected source code (tag/branch/commit or direct URL)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the vulnerability, including how an attacker might exploit it

### Response Timeline

- **Initial Response**: Within 48 hours of receiving your report
- **Status Update**: Within 7 days with an assessment of the vulnerability
- **Fix Timeline**: Depends on severity and complexity
  - Critical: Within 7 days
  - High: Within 14 days
  - Medium: Within 30 days
  - Low: Within 60 days

### What to Expect

After you submit a report:

1. We will acknowledge receipt of your vulnerability report
2. We will investigate and validate the vulnerability
3. We will work on a fix and coordinate disclosure timing with you
4. We will release a security advisory and patched version
5. We will publicly acknowledge your responsible disclosure (if desired)

## Security Best Practices

### For Users

When using DAL Prompt Generator:

- **Input Validation**: Always validate and sanitize Excel input files before processing
- **File Permissions**: Ensure input and output directories have appropriate permissions
- **Sensitive Data**: Never include sensitive data (credentials, PII) in Excel specifications
- **Code Review**: Always review AI-generated code before deploying to production
- **Dependencies**: Keep all dependencies up to date using `pip install --upgrade`
- **Virtual Environment**: Always use a virtual environment to isolate dependencies

### For Contributors

When contributing code:

- **Input Sanitization**: Validate and sanitize all user inputs
- **Path Traversal**: Use `os.path.join()` and validate paths to prevent directory traversal
- **Code Injection**: Never use `eval()` or `exec()` on user-provided data
- **Dependencies**: Only add well-maintained, trusted dependencies
- **Secrets**: Never commit credentials, API keys, or sensitive data
- **Logging**: Ensure logs don't contain sensitive information

### Known Security Considerations

#### Excel File Processing

- Excel files are processed using `pandas` and `openpyxl`
- Malicious Excel files could potentially exploit vulnerabilities in these libraries
- Always use trusted Excel files from known sources
- Keep dependencies updated to receive security patches

#### File System Operations

- The tool creates directories and writes files to the file system
- Ensure proper permissions on input/output directories
- Validate file paths to prevent directory traversal attacks

#### Generated Code

- Generated prompts create code that interacts with AWS DynamoDB
- Always review generated code for security issues before deployment
- Ensure proper IAM permissions and least privilege access
- Use AWS credentials securely (environment variables, IAM roles)

#### Template Injection

- Templates use `{{variable}}` placeholders for substitution
- User input is substituted into templates
- Malicious input could potentially inject unwanted content
- Input validation helps mitigate this risk

## Security Updates

Security updates will be released as:

- Patch versions for minor security fixes (e.g., 2.0.1)
- Minor versions for moderate security fixes (e.g., 2.1.0)
- Major versions for significant security changes (e.g., 3.0.0)

Subscribe to repository releases to receive notifications of security updates.

## Vulnerability Disclosure Policy

We follow responsible disclosure practices:

1. **Private Disclosure**: Security issues are initially disclosed privately
2. **Fix Development**: We develop and test a fix
3. **Coordinated Release**: We coordinate release timing with the reporter
4. **Public Disclosure**: After a fix is released, we publish a security advisory
5. **Credit**: We acknowledge security researchers who report vulnerabilities responsibly

## Security Hall of Fame

We recognize and thank security researchers who help keep DAL Prompt Generator secure:

<!-- Security researchers will be listed here -->

*No vulnerabilities have been reported yet.*

## Scope

### In Scope

The following are within the scope of our security policy:

- Code injection vulnerabilities
- Path traversal vulnerabilities
- Dependency vulnerabilities
- Input validation issues
- File system security issues
- Template injection vulnerabilities

### Out of Scope

The following are outside the scope:

- Social engineering attacks
- Physical attacks
- Denial of service attacks on local systems
- Issues in third-party dependencies (report to the dependency maintainers)
- Issues in AI-generated code (users are responsible for reviewing generated code)

## Additional Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Python Security Best Practices](https://python.readthedocs.io/en/stable/library/security_warnings.html)
- [AWS Security Best Practices](https://aws.amazon.com/security/best-practices/)
- [Pandas Security Considerations](https://pandas.pydata.org/docs/user_guide/io.html#security-considerations)

## Contact

For security-related questions or concerns that are not vulnerabilities, please open a regular GitHub issue or contact the maintainers.

---

**Last Updated**: March 2, 2026

Thank you for helping keep DAL Prompt Generator and its users safe!
