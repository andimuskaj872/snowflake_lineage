# Contributing to Snowflake Lineage Explorer

Thank you for your interest in contributing to the Snowflake Lineage Explorer! This document provides guidelines for contributing to this project.

## 🚀 Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/snowflake_lineage.git
   cd snowflake_lineage
   ```
3. **Set up the development environment**:
   ```bash
   # Using uv (recommended)
   uv sync
   
   # Or using pip
   pip install -r requirements.txt
   ```

## 🛠️ Development Setup

### Prerequisites
- Python 3.9+
- Access to a Snowflake account
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Configuration
1. Copy the example config file:
   ```bash
   cp snowflake_config.toml.example snowflake_config.toml
   ```
2. Add your Snowflake credentials to `snowflake_config.toml`

### Running the Application
```bash
# Using uv
uv run streamlit run app.py

# Using pip (with activated virtual environment)
streamlit run app.py
```

## 🐛 Reporting Issues

When reporting issues, please include:

- **Environment details**: Python version, Snowflake connector version, OS
- **Steps to reproduce**: Clear, step-by-step instructions
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Error messages**: Full error messages and stack traces
- **Screenshots**: If applicable, especially for UI issues

## 💡 Suggesting Features

We welcome feature suggestions! Please:

1. **Check existing issues** to avoid duplicates
2. **Describe the use case** clearly
3. **Explain the benefit** to users
4. **Consider implementation complexity**

## 🔧 Contributing Code

### Before You Start
- Check the **issue tracker** for existing work
- **Open an issue** to discuss major changes
- **Keep changes focused** - one feature/fix per PR

### Code Style
- Follow **PEP 8** Python style guidelines
- Use **descriptive variable names**
- Add **comments** for complex logic
- Keep **functions focused** and reasonably sized

### Making Changes

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write clear, focused commits
   - Test your changes thoroughly
   - Update documentation if needed

3. **Test your changes**:
   ```bash
   # Run the app and test functionality
   uv run streamlit run app.py
   
   # Test with different Snowflake setups if possible
   ```

4. **Commit with clear messages**:
   ```bash
   git commit -m "Add feature: brief description
   
   Detailed explanation of what this commit does and why.
   Include any breaking changes or special setup required."
   ```

### Pull Request Process

1. **Update documentation** if needed
2. **Test thoroughly** with your Snowflake environment
3. **Create a pull request** with:
   - Clear title and description
   - Reference to related issues
   - Screenshots for UI changes
   - Testing notes

4. **Respond to feedback** promptly
5. **Keep your branch updated** with main

## 📝 Documentation

Help improve documentation by:

- **Fixing typos** and unclear instructions
- **Adding examples** for complex features
- **Updating setup instructions** for different environments
- **Adding troubleshooting** sections

## 🧪 Testing

Currently, testing is manual. Help us by:

- **Testing new features** thoroughly
- **Trying different Snowflake configurations**
- **Testing edge cases** (empty results, permission errors, etc.)
- **Verifying across different operating systems**

## 🏷️ Types of Contributions

We welcome contributions in several areas:

### 🚀 Features
- New lineage analysis capabilities
- Additional export formats
- Performance optimizations
- UI/UX improvements

### 🐛 Bug Fixes
- Connection issues
- Query errors
- UI glitches
- Performance problems

### 📚 Documentation
- Setup instructions
- Usage examples
- Troubleshooting guides
- API documentation

### 🔧 Infrastructure
- CI/CD improvements
- Testing framework
- Build optimizations
- Security enhancements

## 💬 Community Guidelines

- **Be respectful** and inclusive
- **Help others** learn and contribute
- **Ask questions** when unsure
- **Share knowledge** and experiences
- **Give constructive feedback**

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

## 🙋 Questions?

- **Open an issue** for technical questions
- **Check existing issues** for similar questions
- **Read the README** for setup and usage help

Thank you for contributing to make Snowflake Lineage Explorer better! 🎉